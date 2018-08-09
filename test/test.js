'use strict'
const uuidv1 = require('uuid/v1');
const redis = require('bluebird').promisifyAll(require('redis'));
const Promise = require('bluebird');
const expect = require('chai').expect;

// this we will test!
const SharedObject = require('../SharedObject');

function wait(delay) {
    
    return new Promise(resolve => setTimeout(resolve, delay));
}

function createRemoteSharedObject(name) {

    const { fork } = require('child_process');
    const forked = fork(__dirname + '/testWorker.js', [name]);
    const requests = new Map();

    forked.on('message', ({ requestId, action, value, error }) => {

        if (requestId === null && error) {
            throw value;
        }

        const request = requests.get(requestId);
        requests.delete(requestId);
        request && request[error ? 'reject' : 'resolve'](value);
    });

    return new Proxy({}, {
        get: function(obj, action) {

            return async function(...args) {

                const requestId = uuidv1();
                return new Promise((resolve, reject) => {

                    requests.set(requestId, { resolve, reject });
                    forked.send({ requestId, action, args });
                })
                    .timeout(5000, `Action '${action}' has timedout!\nArgs:\n${JSON.stringify(args, null, 2)}`)
                    .finally(() => {
                        requests.delete(requestId);
                    });
            };
        }
    });
}

describe('SharedObjct build on Redis', function() {

    const SO1_ID = 'testSO1';
    let so1, remoteSo1;

    const FOO_JSON = { foo: 42 };

    before(async function() {

        const client = redis.createClient();
        remoteSo1 = createRemoteSharedObject(SO1_ID);
        so1 = new SharedObject(SO1_ID, null, { redisClient: client });
        await Promise.join(so1.readyPromise, remoteSo1.ready());
    });

    describe('Single process basic functions', function() {
        
        it('should save without lock', async function() {

            const result = await so1.save('key1', FOO_JSON);
            expect(result).to.deep.equal({ result: [ 'OK', 'OK' ], lock: undefined });
        });

        it('should load expected data', async function() {

            const result = await so1.load('key1');
            expect(result).to.deep.equal(FOO_JSON);
        });

        it('should clear shared object', async function() {

            const destroyResult = await so1.clear();
            expect(destroyResult).to.equal(2);

            const loadResult = await so1.load('key1');
            expect(loadResult).to.equal(undefined);
        });

        it('should save with lock and unlock afterwards', async function() {

            const firstSaveResult = await so1.save('key1', 'first value with lock', { lock: { ttl: 2 } });
            expect(firstSaveResult.lock, `Key 'key1' should be locked`).to.exist;
            
            const unlockResult = await firstSaveResult.lock.unlock('succesfully unlocked');
            expect(unlockResult).to.equal(true);
        });

        it('should save with lock, fail second save, first save value should be still set', async function() {

            const firstSaveResult = await so1.save('key1', 'first value with lock', { lock: { ttl: 2 } });
            expect(firstSaveResult.lock, `Key 'key1' should be locked`).to.exist;
            
            const { result, locked } = await so1.save('key1', 'second value save should fail');
            expect(result).to.equal('locked');
            expect(locked, `'locked' property shoud exist`).to.exist;

            locked.promise.then(lockResult => {
                expect(lockResult).to.deep.equal({ result: 'unlocked' });
            });

            const loadResult = await so1.load('key1');
            expect(loadResult).to.equal('first value with lock');

            const unlockResult = await firstSaveResult.lock.unlock('succesfully unlocked');
            expect(unlockResult).to.equal(true);
        });
        
        it('should save with lock, the lock should timeout', async function() {

            const firstSaveResult = await so1.save('key1', 'first value with lock', { lock: { ttl: 1 } });
            const secondSaveResult = await so1.save('key1', 'second value save should fail');
            
            const lockResult = await secondSaveResult.locked.promise;
            expect(lockResult).to.deep.equal({ result: 'timeout' });
        });
    });

    xdescribe('Remote process SharedObject', function() {

        it('should save without lock', async function() {

            const result = await remoteSo1.save('key1', FOO_JSON);
            expect(result).to.deep.equal({ result: [ 'OK', 'OK' ] });
        });

        it('should load expected data', async function() {

            const remoteResult = await remoteSo1.load('key1');
            expect(remoteResult).to.deep.equal(FOO_JSON);

            const result = await so1.load('key1');
            expect(result).to.deep.equal(FOO_JSON);
        });

        it('should fail to save locked data', async function() {

            so1.save('key1', 'value from local save', { lock: { ttl: 2 } })
                .then(({ result, lock }) => {
                    
                    console.log('!W! - LOCAL result:', result);
                    // TODO:
                });

            await wait(100);

            try {
                const remoteSaveResult = await remoteSo1.save('key1', FOO_JSON, { lock: { ttl: 2 } });
            } catch(error) {
                console.log('!W! - error:', error);
            }

            console.log('!W! - REMOTE remoteSaveResult:', remoteSaveResult);

            const remoteLoadResult = await remoteSo1.load('key1');
            expect(remoteLoadResult).to.equal('value from local save');

            // expect(firstSaveResult.lock, `Key 'key1' should be locked`).to.exist;

            // const removeResult = await remoteSo1.load('key1');
            // expect(removeResult).to.deep.equal(FOO_JSON);

            // const result = await so1.load('key1');
            // expect(result).to.deep.equal(FOO_JSON);
        });
    });

    after(async function() {

        await Promise.join(
            remoteSo1.kill(),
            so1.clear()
            );
    });
});
