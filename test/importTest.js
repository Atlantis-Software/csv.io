var path = require('path');
var fs = require('fs');
var assert = require('assert');

describe('Import', function() {
  var ImportCsv = require(path.join(__dirname, '..', 'index.js')).importer;
  var testData = fs.readFileSync(path.join(__dirname, 'test.csv'));
  var bigTestData = fs.readFileSync(path.join(__dirname, 'fat.csv'));
  var testDataStream = fs.createReadStream(path.join(__dirname, 'test.csv'));
  var bigTestDataStream = fs.createReadStream(path.join(__dirname, 'fat.csv'));
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

    importCsv.info(testData, function(err, info) {
      assert(!err);
      assert(info);
      assert(info.entriesCount, 5, 'Should have found 5 lines');
      assert(info.dataSize, 5, 'File should weight 458 bytes');
      done();
    });
  });

  it('should parse csv buffer line by line into json object', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;

    importCsv.setLineFn(function(line, cb) {
      assert.deepEqual(line, expectedResult[i++], 'Returned json object line should be correct');
      return cb();
    });

    importCsv.process(testData);
    importCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      done();
    });
  });

  it('should accept multiple non-stream inputs', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;

    importCsv.setLineFn(function(line, cb) {
      assert.deepEqual(line, expectedResult[i++], 'Returned json object line should be correct');
      return cb();
    });

    var subDataSet1 = testData.slice(0, 100);
    var subDataSet2 = testData.slice(100, testData.length);
    importCsv.process(subDataSet1);
    importCsv.process(subDataSet2);
    importCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      done();
    });
  });

  it('should parse csv stream line by line into json object', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;

    importCsv.setLineFn(function(line, cb) {
      assert.deepEqual(line, expectedResult[i++], 'Returned json object line should be correct');
      return cb();
    });

    importCsv.process(testDataStream);
    importCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      done();
    });
  });

  it('should accept only a single input in stream mode', function(done) {
    var importCsv = new ImportCsv(options);
    var i = 0;

    importCsv.setLineFn(function(line, cb) {
      assert.deepEqual(line, expectedResult[i++], 'Returned json object line should be correct');
      return cb();
    });

    importCsv.process(testDataStream);
    importCsv.process(testDataStream);
    importCsv.process(testData);
    importCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      done();
    });
  });

  it('should work with a stream even if the internal buffer get full', function(done) {
    var importCsv = new ImportCsv(options);

    importCsv.setLineFn(function(line, cb) {
      return cb();
    });

    importCsv.process(bigTestDataStream);
    importCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      done();
    });
  });

  it('should work with a buffer even if the internal buffer get full', function(done) {
    var importCsv = new ImportCsv(options);

    importCsv.setLineFn(function(line, cb) {
      return cb();
    });

    importCsv.process(bigTestData);
    importCsv.end()
    .on('error', function(err) {
      assert(false, 'should not pass here');
    })
    .on('finish', function() {
      done();
    });
  });

});
