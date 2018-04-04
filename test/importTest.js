var path = require('path');
var fs = require('fs');
var assert = require('assert');
var stream = require('stream');
var util = require('util');

describe('Import', function() {
  var ImportCsv = require(path.join(__dirname, '..', 'index.js')).importer;
  var testDataPath = path.join(__dirname, 'test.csv');
  var bigTestDataPath = path.join(__dirname, 'fat.csv');
  var invalidTestDataPath = path.join(__dirname, 'invalidData.csv');

  var testData = fs.readFileSync(testDataPath);
  var bigTestData = fs.readFileSync(bigTestDataPath);
  var invalidTestData = fs.readFileSync(invalidTestDataPath);

  var options = {
    columns: [
      {name: 'column1', type: 'string'},
      {name: 'column2', type: 'string'},
      {name: 'column3', type: 'number'},
      {name: 'column4', type: 'string', nullable: true},
      {name: 'column5', type: 'boolean'}
    ]
  };

  var expectedResult = [ { column1: 'SomeTextWithoutQuote1',
    column2: 'Some text with quote1',
    column3: 1.12,
    column4: undefined,
    column5: true },
  { column1: 'SomeTextWithoutQuote2',
    column2: 'Some text with quote2',
    column3: 1.23,
    column4: undefined,
    column5: true },
  { column1: 'SomeTextWithoutQuote3',
    column2: 'Some text with quote3',
    column3: 1.34,
    column4: undefined,
    column5: true },
  { column1: 'SomeTextWithoutQuote4',
    column2: 'Some text with quote4',
    column3: 2,
    column4: undefined,
    column5: true },
  { column1: 'SomeTextWithoutQuote5',
    column2: 'Some text with quote5',
    column3: 3,
    column4: undefined,
    column5: true } ];

  it('should return info containing size and number of lines when passed a buffer', function(done) {
    var importCsv = new ImportCsv(options);

    importCsv.getInfo(testData, function(err, info) {
      assert(!err);
      assert(info);
      assert(info.entriesCount, 5, 'Should have found 5 lines');
      assert(info.dataSize, 458, 'File should weight 458 bytes');
      done();
    });
  });

  it('should parse csv buffer line by line into json object', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;

    var output = importCsv.getOutput(function(line, cb) {
      assert.deepEqual(line, expectedResult[i++], 'Returned json object line should be correct');
      return cb();
    });

    importCsv.write(testData);

    output
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      assert.equal(i, 5, 'should have parsed all lines');
      done();
    });
    importCsv.end();
  });

  it('should accept multiple write operations and fragmented input', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;

    var output = importCsv.getOutput(function(line, cb) {
      assert.deepEqual(line, expectedResult[i++], 'Returned json object line should be correct');
      return cb();
    });

    var subDataSet1 = testData.slice(0, 100);
    var subDataSet2 = testData.slice(100, testData.length);

    importCsv.write(subDataSet1);
    importCsv.write(subDataSet2);

    output
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      assert.equal(i, 5, 'should have parsed all lines');
      done();
    });
    importCsv.end();
  });

  it('input should be pipable', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;
    var testDataStream = fs.createReadStream(testDataPath);

    var output = importCsv.getOutput(function(line, cb) {
      assert.deepEqual(line, expectedResult[i++], 'Returned json object line should be correct');
      return cb();
    });

    testDataStream.pipe(importCsv);

    output
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      assert.equal(i, 5, 'should have parsed all lines');
      done();
    });
  });

  it('output should be pipable', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;
    var testDataStream = fs.createReadStream(testDataPath);
    var sinkStream = new stream.Writable({objectMode: true});
    sinkStream._write = function (chunk, encoding, next) {
      assert.deepEqual(chunk, expectedResult[i++], 'Returned json object line should be correct');
      next();
    };

    var output = importCsv.getOutput();

    testDataStream.pipe(importCsv);
    output.pipe(sinkStream);

    sinkStream
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      assert.equal(i, 5, 'should have parsed all lines');
      done();
    });
  });

  it('should work with streams even if the internal buffer get full', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;
    var testDataStream = fs.createReadStream(bigTestDataPath);
    var sinkStream = new stream.Writable({objectMode: true});
    sinkStream._write = function (chunk, encoding, next) {
      ++i;
      next();
    };

    var output = importCsv.getOutput();

    testDataStream.pipe(importCsv);
    output.pipe(sinkStream);

    sinkStream
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      assert.equal(i, 200, 'should have parsed all lines');
      done();
    });
  });

  it('should work with a buffer even if the internal buffer get full', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;

    var output = importCsv.getOutput(function(line, cb) {
      ++i;
      return cb();
    });

    importCsv.write(bigTestData);

    output
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      assert.equal(i, 200, 'should have parsed all lines');
      done();
    });
    importCsv.end();
  });

  it('should handle backpressure', function(done) {
    var importCsv = new ImportCsv(options);
    var couldPush = true;
    var testDataStream = fs.createReadStream(path.join(__dirname, 'realfat.csv'));
    var i = 0;
    var y = 0;
    var testTransform1 = new stream.Transform({
      transform(chunk, encoding, callback) {
        var dataChunk = chunk;
        var ok = true;
        while (dataChunk.length > 1000) {
          ok = this.push(dataChunk.slice(0, 1000));
          if (!ok) {
            couldPush = false;
          }
          ++i;
          dataChunk = dataChunk.slice(1000, dataChunk.length);
        }
        ++i;
        ok = this.push(dataChunk);
        if (!ok) {
          couldPush = false;
        }
        callback();
      }
    });

    var testTransform2 = new stream.Transform({
      transform(chunk, encoding, callback) {
        var dataChunk = chunk;
        var ok = true;
        while (dataChunk.length > 1000) {
          ok = this.push(dataChunk.slice(0, 1000));
          if (!ok) {
            couldPush = false;
          }
          ++y;
          dataChunk = dataChunk.slice(1000, dataChunk.length);
        }
        ++y;
        ok = this.push(dataChunk);
        if (!ok) {
          couldPush = false;
        }
        callback();
      }
    });


    var sinkStream = new stream.Writable({objectMode: true});
    sinkStream._write = function (chunk, encoding, next) {
      // do nothing
    };

    var output = importCsv.getOutput();

    testDataStream.pipe(testTransform1);
    testTransform1.pipe(testTransform2);
    testTransform2.pipe(importCsv);
    output.pipe(sinkStream);


    sinkStream
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      assert(false, 'should not pass here');
    });


    setTimeout(function() {
      assert(!couldPush, 'back pressure was not handled');
      assert(i > y, 'Previous stream should process more write call');
      done();
    }, 1000);
  });

  it('should use custom formatter', function(done) {
    var options2 = {
      columns: [
        {name: 'column1', type: 'string'},
        {name: 'column2', type: 'number'},
        {name: 'column3', type: 'customType'},
        {name: 'column4', formatter: function(col, val) {
          return 'custom format string';
        }}
      ],
      formatters: {
        number: function(col, val) {
          return val * 2;
        },
        customType: function(col, val) {
          return 'test ' + val;
        }
      }
    };

    var importCsv = new ImportCsv(options2);
    var i = 0;

    var data = Buffer.from('someString;10;someCustomString;50\n');

    var output = importCsv.getOutput(function(line, cb) {
      ++i;
      var shouldBe = {
        column1: 'someString',
        column2: 20,
        column3: 'test someCustomString',
        column4: 'custom format string',
      };
      assert.deepEqual(line, shouldBe, 'Returned json object line should be correct');
      return cb();
    });

    importCsv.write(data);

    output
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      assert.equal(i, 1, 'should have parsed all lines');
      done();
    });
    importCsv.end();
  });

  it('should parse csv buffer and emit an error on invalid data', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;

    var output = importCsv.getOutput(function(line, cb) {
      assert.deepEqual(line, expectedResult[i++], 'Returned json object line should be correct');
      return cb();
    });

    importCsv.write(invalidTestData);

    output
    .on('error', function(err) {
      assert(err.message, 'bad data is not a valid value for column column3 of type number');
      done();
    })
    .on('finish', function() {
      assert(false, 'should not pass here');
    });
    importCsv.end();
  });

  it('should parse csv stream and emit an error on invalid data', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;
    var invalidTestDataStream = fs.createReadStream(invalidTestDataPath);

    var output = importCsv.getOutput(function(line, cb) {
      assert.deepEqual(line, expectedResult[i++], 'Returned json object line should be correct');
      return cb();
    });

    invalidTestDataStream.pipe(importCsv);

    output
    .on('error', function(err) {
      assert(err.message, 'bad data is not a valid value for column column3 of type number');
      done();
    })
    .on('finish', function() {
      assert(false, 'should not pass here');
    });
  });

});
