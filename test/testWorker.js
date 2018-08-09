'use strict'

const redis = require('bluebird').promisifyAll(require('redis'));
const SharedObject = require('../SharedObject');
const [/*ignore*/, /*ignore*/, sharedObjectName] = process.argv;

const so = new SharedObject(sharedObjectName, null, { redisClient: redis.createClient() });

process.on('message', async ({ requestId, action, args }) => {

    switch (action) {

        case 'kill': {
            so.redisClient.quit();
            so.redisClientSub.quit();
            process.send({ requestId, action, value: true });
            process.exit(0);
        }
        break;

        default: 
            try {
                const result = await so[action].apply(so, args);
                process.send({ requestId, action, value: result });
            } catch (error) {
                process.send({ requestId, action, error: true, value: error });
            }
        break;
    }
});
