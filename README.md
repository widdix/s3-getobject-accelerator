# S3 GetObject Accelerator

Get large objects from S3 by using parallel byte-range fetches/parts to improve performance.

Install:

```bash
npm i s3-getobject-accelerator
```

Use:

```js
const AWS = require('aws-sdk');
const {createWriteStream} = require('node:fs');
const {pipeline} = require('node:stream');
const {download} = require('s3-getobject-accelerator');

const s3 = new AWS.S3({apiVersion: '2006-03-01'});

pipeline(
  download(s3, {bucket: 'bucket', key: 'key', version: 'optional version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
  createWriteStream('/tmp/test'),
  (err) => {
    if (err) {
      console.error('something went wrong', err);
    } else {
      console.log('done');
    }
  }
);
```

Get insights into the part downloads:

```js
const AWS = require('aws-sdk');
const {createWriteStream} = require('node:fs');
const {pipeline} = require('node:stream');
const {download} = require('s3-getobject-accelerator');

const s3 = new AWS.S3({apiVersion: '2006-03-01'});

const d = download(s3, {bucket: 'bucket', key: 'key', version: 'optional version'}, {partSizeInMegabytes: 8, concurrency: 4});
d.on('part:downloading', ({partNo}) => {
  console.log('start downloading part', partNo);
});
d.on('part:downloaded', ({partNo}) => {
  console.log('part downloaded, write to disk next in correct order', partNo);
});
d.on('part:done', ({partNo}) => {
  console.log('part written to disk', partNo);
});
pipeline(
  d.readStream(),
  createWriteStream('/tmp/test'),
  (err) => {
    if (err) {
      console.error('something went wrong', err);
    } else {
      console.log('done');
    }
  }
);
```

API:

```js
download(
  s3,                                  // AWS.S3 (v2 SDK)
  {
    bucket: 'bucket',                  // string
    key: 'key',                        // string
    version: 'version'                 // optional string
  },
  {
    partSizeInMegabytes: 8,                       // optional number > 0: if not specified, parts are downloaded as they were uploaded
    concurrency: 4,                               // number > 0
    waitForWriteBeforeDownloladingNextPart: false // optional boolean
  }
) : {
  readStream(),                         // ReadStream (see https://nodejs.org/api/stream.html#class-streamreadable)
  partsDownloading(),                   // number
  addListener(eventName, listener),     // see https://nodejs.org/api/events.html#emitteraddlistenereventname-listener
  off(eventName, listener),             // see https://nodejs.org/api/events.html#emitteroffeventname-listener
  on(eventName, listener),              // see https://nodejs.org/api/events.html#emitteroneventname-listener
  once(eventName, listener),            // see https://nodejs.org/api/events.html#emitteronceeventname-listener
  removeListener(eventName, listener)   // https://nodejs.org/api/events.html#emitterremovelistenereventname-listener
}
```

## Considerations

* Typical sizes `partSizeInMegabytes` are 8 MB or 16 MB. If objects are uploaded using a multipart upload, it’s a good practice to download them in the same part sizes ( do not specify `partSizeInMegabytes`), or at least aligned to part boundaries, for best performance (see https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html).
* The default S3 client sets `maxSockets` to 50. Therefore, a `concurrency` > 50 requires changes to the S3 client configuration (see https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/node-configuring-maxsockets.html).
