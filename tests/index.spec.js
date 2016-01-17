var SqlGenerator = require('../')
  , DbCriteria = require('@naujs/db-criteria')
  , _ = require('lodash');

describe('SqlGenerator', () => {
  var generator;

  beforeEach(() => {
    generator = new SqlGenerator();
  });

  describe('#select', () => {
    it('should return select statement', () => {
      var criteria = new DbCriteria();
      criteria.where('a', 1);
      criteria.where('b', 2);

      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test WHERE (a = 1 AND b = 2)');
    });

    it('should return select statement even if there is no criteria provided', () => {
      var result = generator.select('test');
      expect(result).toEqual('SELECT * FROM test');
    });

    it('should process OR', () => {
      var criteria = new DbCriteria();
      criteria.where('a', 1);
      criteria.where('b', 2);
      criteria.where('c', 3, true);

      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test WHERE (a = 1 AND b = 2 OR c = 3)');
    });

    it('should process nested DbCriteria query', () => {
      var criteria = new DbCriteria();
      var nestedCriteria = new DbCriteria();
      nestedCriteria.where('d', 3);
      nestedCriteria.where('e', 4);

      criteria.where('a', 1);
      criteria.where('b', 2);
      criteria.where('c', 3, true);
      criteria.where(nestedCriteria, true);

      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test WHERE (a = 1 AND b = 2 OR c = 3 OR (d = 3 AND e = 4))');
    });

    it('should process complex conditions passed to DbCriteria constructor', () => {
      var criteria = new DbCriteria({
        where: {
          and: {
            or: {
              a: 1,
              b: 2
            },
            c: 3
          }
        }
      });

      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test WHERE (((a = 1 OR b = 2) AND c = 3))');

      criteria = new DbCriteria({
        where: {
          a: 1,
          b: 2,
          or: {
            c: 3,
            d: 4,
            and: {
              e: 5,
              f: 6,
              or: {
                g: 7,
                h: 8
              }
            }
          }
        }
      });
      result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test WHERE (a = 1 AND b = 2 AND (c = 3 OR d = 4 OR (e = 5 AND f = 6 AND (g = 7 OR h = 8))))');
    });

    it('should support multiple conditions for the same field', () => {
      var criteria = new DbCriteria();
      criteria.where('a', 1);
      criteria.where('a', 2);
      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test WHERE (a = 1 AND a = 2)');

      criteria = new DbCriteria();
      criteria.where('a', [
        criteria.gte(10),
        criteria.lte(100)
      ]);
      result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test WHERE (a >= 10 AND a <= 100)');

      criteria = new DbCriteria();
      criteria.where('a', [
        criteria.lte(10),
        criteria.gte(100)
      ], true);
      result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test WHERE (a <= 10 OR a >= 100)');
    });

    it('should process IN', () => {
      var criteria = new DbCriteria();
      criteria.where('a', criteria.in([1, 2, 3]));

      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test WHERE (a IN (1, 2, 3))');
    });

    it('should process NOT IN', () => {
      var criteria = new DbCriteria();
      criteria.where('a', criteria.nin([1, 2, 3]));

      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test WHERE (a NOT IN (1, 2, 3))');
    });

    it('should process normal operators', () => {
      var operators = _.chain(SqlGenerator.OPERATORS).keys().without('in', 'nin', 'eq').value();

      _.each(operators, (operator) => {
        var criteria = new DbCriteria();
        criteria.where('a', criteria[operator](1));

        var result = generator.select('test', criteria);
        expect(result).toEqual('SELECT * FROM test WHERE (a ' + SqlGenerator.OPERATORS[operator] + ' 1)');
      });
    });

    it('should use fields in the criteria', () => {
      var criteria = new DbCriteria();
      criteria.where('a', 1);
      criteria.fields('b', 'c', 'd');

      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT b, c, d FROM test WHERE (a = 1)');
    });

    it('should support order', () => {
      var criteria = new DbCriteria();
      criteria.order('a', true);
      criteria.order('b', false);

      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test ORDER BY a ASC, b DESC');
    });

    it('should support offset', () => {
      var criteria = new DbCriteria();
      criteria.offset(10);

      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test OFFSET 10');
    });

    it('should support limit', () => {
      var criteria = new DbCriteria();
      criteria.limit(10);

      var result = generator.select('test', criteria);
      expect(result).toEqual('SELECT * FROM test LIMIT 10');
    });
  });

  describe('#insert', () => {
    it('should insert a single row', () => {
      var result = generator.insert('test', {
        'a': 1,
        'b': 2
      });

      expect(result).toEqual('INSERT INTO test (a, b) VALUES (1, 2)');
    });

    it('should insert a single row using noQuote option', () => {
      var result = generator.insert('test', {
        'a': 1,
        'b': 'GET_DATE()'
      }, {
        noQuote: ['b']
      });

      expect(result).toEqual('INSERT INTO test (a, b) VALUES (1, GET_DATE())');
    });

    it('should insert multiple rows', () => {
      var result = generator.insert('test', [
        {
          'a': 1,
          'b': 2
        },
        {
          'a': 3,
          'b': 4
        }
      ]);

      expect(result).toEqual('INSERT INTO test (a, b) VALUES (1, 2), (3, 4)');
    });
  });

  describe('#update', () => {
    it('should update a row', () => {
      var result = generator.update('test', {
        'a': 1,
        'b': 2
      });

      expect(result).toEqual('UPDATE test SET a = 1, b = 2');
    });

    it('should update a row using WHERE', () => {
      var criteria = new DbCriteria();
      criteria.where('c', 1);

      var result = generator.update('test', {
        'a': 1,
        'b': 2
      }, criteria);

      expect(result).toEqual('UPDATE test SET a = 1, b = 2 WHERE (c = 1)');
    });

    it('should update a row using noQuote', () => {
      var result = generator.update('test', {
        'a': 1,
        'b': 'GET_DATE()'
      }, null, {
        noQuote: 'b'
      });

      expect(result).toEqual('UPDATE test SET a = 1, b = GET_DATE()');
    });
  });

  describe('#delete', () => {
    it('should delete all rows', () => {
      var result = generator.delete('test');

      expect(result).toEqual('DELETE FROM test');
    });

    it('should delete rows using criteria', () => {
      var criteria = new DbCriteria();
      criteria.where('c', 1);
      var result = generator.delete('test', criteria);

      expect(result).toEqual('DELETE FROM test WHERE (c = 1)');
    });

  });

  describe('psql engine', () => {
    beforeEach(() => {
      generator = new SqlGenerator(SqlGenerator.PSQL);
    });

    describe('#insert', () => {
      it('should return all fields after inserting', () => {
        var result = generator.insert('test', {
          'a': 1,
          'b': 2
        });

        expect(result).toEqual('INSERT INTO test (a, b) VALUES (1, 2) RETURNING *');
      });
    });
  });

});
