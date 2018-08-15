'use strict';

const EventEmitter = require('events');
const Promise = require('bluebird');
const uuidv1 = require('uuid/v1');

const UUID = Symbol('uuid');
const FULLNAME = Symbol('fullname');
const LOCKS = Symbol('locks');
const LOCK_RESOLVE = Symbol('lockResolve');
const LOCK = Symbol('lockFn');
const GET_LOCK = Symbol('getLockFn');
const SUB_CHANNEL = 'SO:service';
const META_KEY = '__META__';
const META_PREFIX = Symbol('metaPrefix');
const QUESTIONS = Symbol('questions');

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

    const fullKey = this.buildLockKey(key);
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
                await this.redisClient.delAsync(fullKey);
                return this.notify({
                    type: 'unlock',
                    key,
                    value: result
                });
            }
        }
    };
    return [newChain, lock];
}

/**
 * private SharedObject::getLock
 * @param  {Object} lockData
 * @return {Object}
 */
function getLock(lockData) {

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
            };
        }).timeout(timeSpan + this.timespanTreshold)
            .catch(Promise.TimeoutError, timeoutError => {
                lock.locked = false;
                return Promise.resolve({ result: 'timeout' });
            }) :
        // the time has already expired
        Promise.resolve({ result: 'timeout' });

    lock.promise = lockPromise;
    return lock;
}

/**
 * private helper - counts connected SharedObject instances
 * @param  {string} clientsString result of command CLIENTS LIST
 * @param  {string} namePrefix the id of SharedObject
 * @return {number}
 */
function countClients(clientsString, namePrefix) {

    let count = 0;
    let lastIndex = 0;

    while (~(lastIndex = clientsString.indexOf(namePrefix, lastIndex))) {
        count++;
        lastIndex++;
    }

    return count;
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

        this[QUESTIONS] = new Map();
        this[LOCK] = lock.bind(this);
        this[GET_LOCK] = getLock.bind(this);
        this[LOCKS] = new Map();
        this[UUID] = uuid || uuidv1().toString();
        this[FULLNAME] = ((prefix && prefix + ':') || '') + id;
        this[META_PREFIX] = this[FULLNAME] + ':' + META_KEY + ':';
        this.redisClient = redisClient;

        if (!redisClientSub || !redisClientSubName) {
            redisClientSub = this.redisClient.duplicate();
            redisClientSubName = SUB_CHANNEL + ':' + this[FULLNAME];
            redisClientSub.subscribe(redisClientSubName);
        }

        this.redisClientSub = redisClientSub;
        this.redisClientSubName = redisClientSubName;

        // TODO:
        this.redisClient.on('error', function(err) {
            console.log('CLIENT Error:\n', err);
        });

        this.redisClientSub.on('error', function(err) {
            console.log('CLIENT SUB Error:\n', err);
        });

        this.redisClientSub.on('message', (channel, message) => {

            const notification = JSON.parse(message);
            // first, go through (un)locks
            if (notification.type === 'unlock') {
                const foundLock = this[LOCKS].get(notification.key);
                if (foundLock && foundLock.locked) {
                    foundLock[LOCK_RESOLVE]('unlocked');
                }
                this[LOCKS].delete(notification.key);
            }

            if (notification.type === 'question' && notification.sender !== this[UUID]) {

                let unanswered = this.listenerCount('question');
                const answers = [];

                const question = notification;
                question.answer = (data) => {
                    answers.push(data);
                    unanswered--;
                    if (unanswered === 0) {
                        this.notify({
                            qid: notification.qid,
                            type: 'answer',
                            expectedAnswers: notification.expectedAnswers,
                            answer: answers.length === 1 ? answers[0] : answers
                        });
                    }
                };

                this.emit('question', question);
                return;
            }

            if (notification.type === 'answer' && notification.sender !== this[UUID]) {
                const foundQuestion = this[QUESTIONS].get(notification.qid);
                if (foundQuestion) {
                    foundQuestion.answers = foundQuestion.answers || [];
                    foundQuestion.answers.push(notification);
                    if (foundQuestion.answers.length === notification.expectedAnswers) {
                        this[QUESTIONS].delete(notification.qid);
                        foundQuestion.resolve(foundQuestion.answers);
                    }
                }
                return;
            }


            if (notification.notifySender || notification.sender !== this[UUID]) {
                this.emit('notification', notification);
            }
        });

        // sets the name of redis client to this instance id, so we will be able to track instances with same id
        this.redisClient.clientAsync('setname', this[FULLNAME]).catch(error => { throw error; });
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
    notify(notification, options = {}) {

        const { notifySender } = options;
        const raw = JSON.stringify(Object.assign({}, notification, { sender: this[UUID], notifySender }));
        return this.redisClient.publish(this.redisClientSubName, raw);
    }

    /**
     * Ask other SharedObject instances
     * @return {Promise<Array<Object>>} array of replies from other instances
     */
    async ask(question) {

        const promise = new Promise(async(resolve, reject) => {
            const clients = await this.redisClient.clientAsync('list');
            const otherClientsCount = countClients(clients, this[FULLNAME]) - 1;

            const qid = uuidv1().toString();
            const json = {
                qid,
                type: 'question',
                question,
                expectedAnswers: otherClientsCount
            };

            this[QUESTIONS].set(qid, { resolve, reject, qid });
            await this.notify(json);
        }).timeout(5000 /*TODO: rozmyslet*/);

        return promise;
    }


    buildKey(key) {

        if (!key) {
            throw new Error('Save failed! No key specified!');
        }
        return this[FULLNAME] + ':' + key;
    }

    buildLockKey(key) {

        if (!key) {
            throw new Error('Save failed! No key specified!');
        }
        return this[META_PREFIX] + ':locks:' + key;
    }

    /**
     * Gets the Lock object, even if the key isn't locked.
     * @param  {[type]} key [description]
     * @return {[type]}     [description]
     */
    async getLock(key) {

        const foundLock = this[LOCKS].get(key);
        if (foundLock) {
            return foundLock;
        }

        const fullLockKey = this.buildLockKey(key);
        const rawLock = await this.redisClient.getAsync(fullLockKey);
        if (rawLock) {
            const lockData = JSON.parse(rawLock);
            const lock = this[GET_LOCK](lockData);
            if (lock.locked) {
                this[LOCKS].set(key, lock);
            }
            return lock;
        }

        return {
            options: null,
            promise: Promise.resolve(),
            locked: false // not locked
        };
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
        await this.redisClient.watchAsync(fullKey, fullLockKey);
        const rawLock = await this.redisClient.getAsync(fullLockKey);
        if (rawLock) {
            const lockData = JSON.parse(rawLock);
            const lock = this[GET_LOCK](lockData);
            if (lock.locked) {
                this[LOCKS].set(key, lock);
            }
            // discard the transaction, because there was a lock - unwatch those key and lock
            await this.redisClient.unwatchAsync();
            return {
                result: 'locked',
                locked: lock
            };
        } else {
            let setChain = ttl ?
                this.redisClient.multi().set(fullKey, raw, 'EX', ttl) :
                this.redisClient.multi().set(fullKey, raw);
            if (lock) {
                var [newChain, lockInstance] = this[LOCK](setChain, key, lock);
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
            };
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
            throw new Error('Load failed! No key specified!');
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
