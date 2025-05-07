const http = require('node:http');
const assert = require('node:assert/strict');
const {request} = require('../index.js');

const PORT = 3333;

describe('server', () => {
  describe('data timeout', () => {
    let server;
    before((done) => {
      server = http.createServer({}, (req, res) => {
        res.write('test1');
        setTimeout(() => {
          res.write('test2');
          res.end();
        }, 150);
      });
      server.listen(PORT, done);
    });
    it('happy', (done) => {
      request(http, {
        hostname: '127.0.0.1',
        port: PORT,
        method: 'GET',
        path: '/'
      }, null, {
        dataTimeoutInMilliseconds: 200
      }, {}, done);
    });
    it('timeout', (done) => {
      request(http, {
        hostname: '127.0.0.1',
        port: PORT,
        method: 'GET',
        path: '/'
      }, null, {
        dataTimeoutInMilliseconds: 100
      }, {}, (err) => {
        assert.strictEqual(err.name, 'DataTimeoutError');
        done();
      });
    });
    after((done) => {
      server.close(done);
    });
  });
  describe('read timeout', () => {
    let server;
    before((done) => {
      server = http.createServer({}, (req, res) => {
        res.write('test1');
        setTimeout(() => {
          res.write('test2');
          res.end();
        }, 150);
      });
      server.listen(PORT, done);
    });
    it('happy', (done) => {
      request(http, {
        hostname: '127.0.0.1',
        port: PORT,
        method: 'GET',
        path: '/'
      }, null, {
        readTimeoutInMilliseconds: 200
      }, {}, done);
    });
    it('timeout', (done) => {
      request(http, {
        hostname: '127.0.0.1',
        port: PORT,
        method: 'GET',
        path: '/'
      }, null, {
        readTimeoutInMilliseconds: 100
      }, {}, (err) => {
        assert.strictEqual(err.name, 'ReadTimeoutError');
        done();
      });
    });
    after((done) => {
      server.close(done);
    });
  });
});
