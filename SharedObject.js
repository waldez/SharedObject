'use strict'

const EventEmitter = require('events');
const Promise = require('bluebird');
const uuidv1 = require('uuid/v1');

const UUID = Symbol('uuid');
const VALUE = Symbol('value');
const FULLNAME = Symbol('fullname');
const LOCKS = Symbol('locks');
const LOCK_RESOLVE = Symbol('lockResolve');
const LOCK_METHOD = Symbol('lockFn');
const LOCK_TIMER = Symbol('lockTimer');
const SUB_CHANNEL = 'SO:service';
const META_KEY = '__META__';

// this is the timespan, which is used for retry of save and also to offset a bit local timeout timer
const TIME_SPAN_TRESHOLD = 50;
const MAX_RETRY_COUNT = 5;

// Private methods to be bound onto private Symbol property
/**
 * private SharedObject::lock
 * @param  {Object} chain
 * @param  {string} key
 * @param  {Object} options
 * @return {Array<>}
 */
function lock(chain, key, options = {}) {

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
                // TODO: should I add random number for each lock instance?
                // Q: could happend, that I'll delete the lock from next command?
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

class SharedObject extends EventEmitter {

    /**
     * @param  {string} id
     * @param  {string} [prefix]
     * @param  {Object} options
     * @param  {Object} options.redisClient connected redis client
     * @param  {Object} options.redisClientSub connected redis client subscribed to service channel
     * @param  {Object} options.redisClientSubName name of the service channel
     * @return {SharedObject}
     */
    constructor(id, prefix = null, options = {}) {
    
        if (id === null || typeof id === 'undefined') {
            throw new ReferenceError('Parameter \'id\' is null or undefined!');
        }

        let {
            redisClient,
            redisClientSub,
            redisClientSubName,
            timespanTreshold,
            maxRetryCount,
            uuid
        } = options;
        // everything is
        super();

        this.timespanTreshold = timespanTreshold || TIME_SPAN_TRESHOLD;
        this.maxRetryCount = maxRetryCount || MAX_RETRY_COUNT;

        this[LOCK_METHOD] = lock.bind(this);
        this[LOCKS] = new Map();
        this[UUID] = uuid || uuidv1().toString();
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
                if (foundLock && foundLock.locked) {
                    foundLock[LOCK_RESOLVE]('unlocked');
                }
                this[LOCKS].delete(notification.key);
            }

            // If I'm not the sender, process the notification...
            if (notification.sender !== this[UUID]) {
                this.emit('notification', notification);
            }
        });
    }

    get fullName() {

        return this[FULLNAME];
    }

    get instanceUUID() {

        return this[UUID];
    }

    /**
     * Notify other clients wia channel message
     * @param  {Object} notification
     * @return {Promise}
     */
    notify(notification) {

        const raw = JSON.stringify(Object.assign({}, notification, { sender: this[UUID] }));
        return this.redisClient.publish(this.redisClientSubName, raw);
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

    /**
     * Save key-value
     * @param  {string} key
     * @param  {*} value
     * @param  {Object} options
     * @param  {Object} options.lock if defined, save item will be locked accordingly
     * @param  {number} options.lock.ttl time to live for the lock
     * @param  {number} options.ttl time to live for item
     * @param  {number} options.tries reserved for internal purposes
     * @param  {number} options.maxRetryCount if race condition happens, this is maximum number of tries
     *                                        before the function fails. If set to 0 and race condition happens,
     *                                        the function fail immediately
     * @return {Promise}
     */
    async save(key, value, options = {}) {

        const raw = JSON.stringify(value);
        const fullKey = this.buildKey(key);
        const fullLockKey = this.buildLockKey(key);
        const { lock, tries = 0, maxRetryCount = this.maxRetryCount, ttl } = options;

        if (tries > maxRetryCount) {
            throw new Error('Exceeded maximum number of save tries!');
        }

        // begin transaction (watch the key and the lock for this key)
        const watchResult = await this.redisClient.watchAsync(fullKey, fullLockKey);
        const rawLock = await this.redisClient.getAsync(fullLockKey);
        if (rawLock) {
            const lockData = JSON.parse(rawLock);
            lockData.expire = new Date(lockData.expire);

            const timeSpan = lockData.expire - new Date();
            const lock = {
                options: lockData,
                locked: false // the lock could be already expired, so the default is false
            };
            const lockPromise = timeSpan > 0 ?
                // there is still time to live
                new Promise((resolve/*, reject*/) => {
                    lock.locked = true;
                    lock[LOCK_RESOLVE] = result => {
                        lock.locked = false;
                        resolve({ result });
                    }
                }).timeout(timeSpan + this.timespanTreshold)
                    .catch(Promise.TimeoutError, timeoutError => {
                        lock.locked = false;
                        return Promise.resolve({ result: 'timeout' });
                    }) :
                // the time has already expired
                Promise.resolve({ result: 'timeout' });

            lock.promise = lockPromise;
            if (lock.locked) {
                this[LOCKS].set(key, lock);
            }
            // discard the transaction, because there was a lock - unwatch those key and lock
            await this.redisClient.unwatchAsync();
            return {
                result: 'locked',
                locked: lock
            }
        } else {
            let setChain = ttl ?
                this.redisClient.multi().set(fullKey, raw, 'EX', ttl) :
                this.redisClient.multi().set(fullKey, raw);
            if (lock) {
                var [newChain, lockInstance] = this[LOCK_METHOD](setChain, key, lock);
                // update the chain
                setChain = newChain;
            }

            const result = await setChain.execAsync();
            if (result === null) {
                return Promise
                .delay(this.timespanTreshold)
                .then(this.save.bind(this,
                    key,
                    value,
                    Object.assign(options, { tries: tries + 1 })
                    ));
            }

            return {
                result,
                lock: lockInstance
            }
        }
    }

    async clear() {

        const keys = await this.redisClient.keysAsync(this[FULLNAME] + ':*');
        if (keys && keys.length) {
            return this.redisClient.delAsync(keys);
        }
    }

    /**
     * Load item
     * @param  {string} key
     * @return {Promise}
     */
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
