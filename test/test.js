'use strict'
const uuidv1 = require('uuid/v1');
const redis = require('bluebird').promisifyAll(require('redis'));
const Promise = require('bluebird');
const expect = require('chai').expect;

// this we will test!
const SharedObject = require('../SharedObject');

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

    describe('Single instace basic functions', function() {
        
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

        xit('should do it', async function() {

        });
    });

    after(async function() {

        await Promise.join(
            remoteSo1.kill(),
            so1.clear()
            );
    });
});
