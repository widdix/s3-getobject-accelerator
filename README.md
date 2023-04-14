# S3 GetObject Accelerator

Get large objects from S3 by using parallel byte-range fetches/parts without the AWS SDK to improve performance.

> We measured a troughoput of 6.5 Gbit/s on an m5zn.6xlarge in eu-west-1 using this lib with this settings: `{concurrency: 64}`.

Install:

```bash
npm i s3-getobject-accelerator
```

Use:

```js
const {createWriteStream} = require('node:fs');
const {pipeline} = require('node:stream');
const {download} = require('s3-getobject-accelerator');

pipeline(
  download({bucket: 'bucket', key: 'key', version: 'optional version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
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

Get insights into the part downloads and write to filew directly without stream:

```js
const {createWriteStream} = require('node:fs');
const {download} = require('s3-getobject-accelerator');

const d = download({bucket: 'bucket', key: 'key', version: 'optional version'}, {partSizeInMegabytes: 8, concurrency: 4});

d.on('part:downloading', ({partNo}) => {
  console.log('start downloading part', partNo);
});
d.on('part:downloaded', ({partNo}) => {
  console.log('part downloaded, write to disk next in correct order', partNo);
});
d.on('part:done', ({partNo}) => {
  console.log('part written to disk', partNo);
});

d.file('/tmp/test', (err) => {
  if (err) {
    console.error('something went wrong', err);
  } else {
    console.log('done');
  }
});
```

API:

```js
download(
  {
    bucket: 'bucket',                  // string
    key: 'key',                        // string
    version: 'version'                 // optional string
  },
  {
    partSizeInMegabytes: 8,                        // optional number > 0: if not specified, parts are downloaded as they were uploaded
    concurrency: 4,                                // number > 0
    connectionTimeoutInMilliseconds: 3000          // optional number >= 0: zero means no timeout
  }
) : {
  readStream(),                         // ReadStream (see https://nodejs.org/api/stream.html#class-streamreadable)
  file(path),
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
* Keep in mind that you pay per GET request to Amazon S3. The smaller the parts, the more expensive a download is.
