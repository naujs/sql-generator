var SqlGenerator = require('../')
  , DbCriteria = require('@naujs/db-criteria')
  , _ = require('lodash');

describe('SqlGenerator', () => {
  var generator;

  describe('#select', () => {
    beforeEach(() => {
      generator = new SqlGenerator();
    });

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
});
