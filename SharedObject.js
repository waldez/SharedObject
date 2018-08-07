'use strict'

const EventEmitter = require('events');
const Promise = require('bluebird');
const uuidv1 = require('uuid/v1');

const UUID = Symbol('uuid');
const VALUE = Symbol('value');
const META = Symbol('meta');
const META_KEY = 'λ';
const FULLNAME = Symbol('fullname');
const LOCKS = Symbol('locks');
const SUB_CHANNEL = 'SO:service';

class SharedObject extends EventEmitter {

    constructor(id = 'SO', prefix = null, options = {}) {
    
        let {
            redisClient,
            redisClientSub,
            redisClientSubName
        } = options;
        super();

        this[LOCKS] = new Map();
        this[UUID] = uuidv1().toString();
        this[FULLNAME] = ((prefix && prefix + ':') || '') + id;
        this.redisClient = redisClient;

        if (!redisClientSub || !redisClientSubName) {
            redisClientSub = this.redisClient.duplicate();
            redisClientSubName = SUB_CHANNEL + ':' + this[FULLNAME];
            redisClientSub.subscribe(redisClientSubName);
        }

        this.redisClientSub = redisClientSub;
        this.redisClientSubName = redisClientSubName;

        // TODO:
        this.redisClient.on("error", function (err) {
            console.log("CLIENT Error:\n", err);
        });

        this.redisClientSub.on("error", function (err) {
            console.log("CLIENT SUB Error:\n", err);
        });


        this.redisClientSub.on("message", (channel, message) => {

            const notification = JSON.parse(message);

            // first, go through (un)locks
            if (notification.type === 'unlock') {
                const foundLock = this[LOCKS].get(notification.key);
                if (foundLock) {
                    foundLock.resolve({ result: 'unlocked' });
                    this[LOCKS].delete(notification.key);
                }
            }

            // If I'm not the sender, process the notification...
            if (notification.sender !== this[UUID]) {
                this.onNotification(notification);
            }
        });

        this.readyPromise = this.loadMeta().catch(error => { throw new Error(error) });
    }

    async saveMeta() {

        await this.save(META_KEY, this[META]);
    }

    async loadMeta() {

        this[META] = await this.load(META_KEY);
        if (!this[META]) {
            this[META] = { props: {} };
        }
    }

    get fullName() {

        return this[FULLNAME];
    }

    notify(notification) {

        const raw = JSON.stringify(Object.assign({}, notification, { sender: this[UUID] }));
        return this.redisClient.publish(this.redisClientSubName, raw);
    }

    onNotification(notification) {

        console.log(`!W! - ===================== NOTIFICATION =====================\n`);
        console.log('!W! - notification:', notification);
    }

    lock(chain, key, options = {}) {

        const fullKey = this[FULLNAME] + ':' + META_KEY + ':locks:' + key;
        const additional = { owner: this[UUID] };
        let expire;

        if (options.ttl) {
            const expire = new Date();
            expire.setSeconds(expire.getSeconds() + options.ttl);
            additional.expire = expire;
        }

        const raw = JSON.stringify(Object.assign({}, options, additional));
        const newChain = chain.set(fullKey, raw, 'NX', 'EX', options.ttl);
        const lock = {
            unlock: async result => {
                if (expire && expire < new Date()) {
                    // already expired, nothing left to do
                    return;
                } else {
                    const delResult = await this.redisClient.delAsync(fullKey);
                    return this.notify({
                        type: 'unlock',
                        key,
                        value: result
                    });
                }
            }
        }
        return [newChain, lock];
    }

    unlock(key, result) {
        
        const fullKey = this[FULLNAME] + ':' + META_KEY + ':locks:' + key;
    }

    buildKey(key) {

        if (!key) {
            throw new Error(`Save failed! No key specified!`);
        }
        return this[FULLNAME] + ':' + key;
    }
    
    buildLockKey(key) {

        if (!key) {
            throw new Error(`Save failed! No key specified!`);
        }
        return this[FULLNAME] + ':' + META_KEY + ':locks:' + key;
    }

    async save(key, value, options = {}) {

        const raw = JSON.stringify(value);
        const fullKey = this.buildKey(key);
        const fullLockKey = this.buildLockKey(key);
        const { lock } = options;
        const SPAN_TRESHOLD = 50;

        try {
            const watchResult = await this.redisClient.watchAsync(fullKey, fullLockKey);
            const rawLock = await this.redisClient.getAsync(fullLockKey);

            if (rawLock) {
                const lockData = JSON.parse(rawLock);
                lockData.expire = new Date(lockData.expire);

                const timeSpan = lockData.expire - new Date();
                const lock = { options: lockData };
                const lockPromise = timeSpan > 0 ? new Promise((resolve, reject) => {

                    lock.locked = true;
                    // TODO: module private
                    lock.timer = setTimeout(() => {
                        reject({ result: 'timeout' });
                        lock.locked = false;
                        // TODO: module private
                        lock.resolve = ()=>{};
                        // TODO: module private
                        lock.reject = ()=>{};
                    }, timeSpan + SPAN_TRESHOLD);

                    lock.resolve = result => {
                        clearTimeout(lock.timer);
                        lock.timer = null;
                        // TODO: module private
                        lock.resolve = ()=>{};
                        // TODO: module private
                        lock.reject = ()=>{};
                        resolve({ result });
                    }

                }) : Promise.reject({ result: 'timeout' });

                lock.promise = lockPromise;
                this[LOCKS].set(key, lock);
                // UNwatch
                this.redisClient.multi().discard();

                return {
                    result: 'locked',
                    locked: lock
                }
            } else {

                let setChain = this.redisClient.multi().set(fullKey, raw);
                // if there is not lock set from user, just try to set lock with 0 ttl and NX property, to see
                // if this property has been locked (see more at: https://redis.io/commands/set)
                // let [newChain, lockInstance] = this.lock(setChain, key, lock || { ttl: 0 });
                let [newChain, lockInstance] = this.lock(setChain, key, lock);
                
                // update the chain
                setChain = newChain;

                if (!this[META].props[key]) {
                    var newMeta = Object.assign({}, this[META]);
                    newMeta.props = Object.assign({}, this[META].props, { [key]: this[UUID] });
                    setChain = setChain.set(this[FULLNAME] + ':' + META_KEY, JSON.stringify(newMeta));
                }

                const result = await setChain.execAsync();
                if (newMeta) {
                    this[META] = newMeta;
                    await this.notify({
                        type: 'change',
                        key: META_KEY,
                        value: newMeta
                    });
                }

                return {
                    result,
                    lock: lockInstance
                }
            }
        } catch(watchError) {
            console.log(`!W! - ===================== watch error =====================\n`);
            console.log('!W! - watchError:', watchError);
        }
    }

    async destroy() {

        const keys = await this.redisClient.keysAsync(this[FULLNAME] + ':*');
        await this.redisClient.del(keys);
        // TODO: send notification!
        
        // this.redisClient.quit();
        // this.redisClientSub.quit();
    }

    async load(key) {

        if (!key) {
            throw new Error(`Load failed! No key specified!`);
        }

        key = this[FULLNAME] + ':' + key;
        const rawValue = await this.redisClient.getAsync(key);
        
        if (!rawValue) {
            return void 0;
        }

        return JSON.parse(rawValue);
    }
}

module.exports = SharedObject;
