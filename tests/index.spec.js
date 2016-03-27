'use strict';

var SqlGenerator = require('../')
  , DbCriteria = require('@naujs/db-criteria')
  , _ = require('lodash')
  , Registry = require('@naujs/registry')
  , ActiveRecord = require('@naujs/active-record');

class Store extends ActiveRecord {}
Store.properties = {
  name: {
    type: ActiveRecord.Types.string
  }
};
Store.relations = {
  products: {
    type: 'hasMany',
    model: 'Product',
    foreignKey: 'store_id'
  },
  banners: {
    type: 'hasMany',
    model: 'Banner',
    foreignKey: 'store_id'
  },
  owner: {
    type: 'belongsTo',
    model: 'Users',
    foreignKey: 'user_id'
  }
};

class Product extends ActiveRecord {}
Product.properties = {
  name: {
    type: ActiveRecord.Types.string
  }
};

Product.relations = {
  'comments': {
    type: 'hasMany',
    model: 'Comment',
    foreignKey: 'product_id'
  },
  'store': {
    type: 'belongsTo',
    model: 'Store',
    foreignKey: 'store_id'
  },
  'tags': {
    type: 'hasManyAndBelongsTo',
    model: 'Tag',
    through: 'ProductTag',
    foreignKey: 'product_id'
  }
};

class Comment extends ActiveRecord {}
Comment.properties = {
  content: {
    type: ActiveRecord.Types.string
  }
};
Comment.relations = {
  'author': {
    type: 'belongsTo',
    model: 'Users',
    foreignKey: 'user_id'
  },
  'product': {
    type: 'belongsTo',
    model: 'Product',
    foreignKey: 'product_id'
  },
  'votes': {
    type: 'hasMany',
    model: 'Vote',
    foreignKey: 'comment_id'
  }
};

class Tag extends ActiveRecord {}
Tag.properties = {
  name: {
    type: ActiveRecord.Types.string
  }
};
Tag.relations = {
  'products': {
    type: 'hasManyAndBelongsTo',
    model: 'Product',
    through: 'ProductTag',
    foreignKey: 'tag_id'
  }
};

class ProductTag extends ActiveRecord {}
ProductTag.relations = {
  'product': {
    type: 'belongsTo',
    model: 'Product',
    foreignKey: 'product_id'
  },
  'tag': {
    type: 'belongsTo',
    model: 'Tag',
    foreignKey: 'tag_id'
  }
};

class User extends ActiveRecord {}
User.properties = {
  name: {
    type: ActiveRecord.Types.string
  }
};
User.relations = {
  'comments': {
    type: 'hasMany',
    model: 'Comment',
    foreignKey: 'user_id'
  },
  'votes': {
    type: 'hasMany',
    model: 'Vote',
    foreignKey: 'user_id'
  },
  'stores': {
    type: 'hasMany',
    model: 'Store',
    foreignKey: 'user_id'
  }
};
User.modelName = 'Users';

class Vote extends ActiveRecord {}
Vote.properties = {
  rating: {
    type: ActiveRecord.Types.number
  }
};

Vote.relations = {
  comment: {
    type: 'belongsTo',
    model: 'Comment',
    foreignKey: 'comment_id'
  },
  author: {
    type: 'belongsTo',
    model: 'User',
    foreignKey: 'user_id'
  }
};

class Banner extends ActiveRecord {}
Banner.properties = {
  image: {
    type: ActiveRecord.Types.string
  }
};

Banner.relations = {
  'store': {
    type: 'belongsTo',
    model: 'Store',
    foreignKey: 'store_id'
  }
};

Registry.setModel(Store);
Registry.setModel(Product);
Registry.setModel(Comment);
Registry.setModel(Tag);
Registry.setModel(User);
Registry.setModel(Vote);
Registry.setModel(ProductTag);
Registry.setModel(Banner);

describe('SqlGenerator', () => {
  var generator, criteria;

  beforeEach(() => {
    generator = new SqlGenerator();
    criteria = new DbCriteria(Store);
  });

  describe('#select', () => {
    it('should return select statement', () => {
      criteria.where('a', 1);
      criteria.where('b', 2);

      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a = 1 AND Store.b = 2)');
    });

    it('should process OR', () => {
      criteria.where('a', 1);
      criteria.where('b', 2);
      criteria.where('c', 3, true);

      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a = 1 AND Store.b = 2 OR Store.c = 3)');
    });

    it('should process nested DbCriteria query', () => {
      var nestedCriteria = new DbCriteria(Store);
      nestedCriteria.where('d', 3);
      nestedCriteria.where('e', 4);

      criteria.where('a', 1);
      criteria.where('b', 2);
      criteria.where('c', 3, true);
      criteria.where(nestedCriteria, true);

      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a = 1 AND Store.b = 2 OR Store.c = 3 OR (Store.d = 3 AND Store.e = 4))');
    });

    it('should process complex conditions passed to DbCriteria constructor', () => {
      criteria = new DbCriteria(Store, {
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

      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (((Store.a = 1 OR Store.b = 2) AND Store.c = 3))');

      criteria = new DbCriteria(Store, {
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
      result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a = 1 AND Store.b = 2 AND (Store.c = 3 OR Store.d = 4 OR (Store.e = 5 AND Store.f = 6 AND (Store.g = 7 OR Store.h = 8))))');
    });

    it('should support multiple or conditions for the same field passed to DbCriteria constructor', () => {
      criteria = new DbCriteria(Store, {
        where: {
          or: [
            {a: 0},
            {a: {gt: 2}}
          ]
        }
      });
      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE ((Store.a = 0 OR Store.a > 2))');
    });

    it('should support multiple and conditions for the same field passed to DbCriteria constructor', () => {
      criteria = new DbCriteria(Store, {
        where: {
          and: [
            {a: 0},
            {a: {gt: 2}}
          ]
        }
      });
      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE ((Store.a = 0 AND Store.a > 2))');
    });

    it('should support multiple nested conditions for the same field passed to DbCriteria constructor', () => {
      criteria = new DbCriteria(Product, {
        where: {
          and: {
            or: [
              {a: 0},
              {a: {gt: 2}}
            ],
            b: 1
          }
        }
      });
      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Product.name AS "Product.name", Product.id AS "Product.id", Product.store_id AS "Product.store_id" FROM Product WHERE (((Product.a = 0 OR Product.a > 2) AND Product.b = 1))');
    });

    it('should support multiple insanely nested conditions for the same field passed to DbCriteria constructor', () => {
      criteria = new DbCriteria(Store, {
        where: {
          and: {
            or: [
              {a: 0},
              {a: {gt: 2}},
              {
                and: [
                  {e: 2},
                  {f: {lt: 3}}
                ]
              }
            ],
            b: 1
          }
        }
      });
      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (((Store.a = 0 OR Store.a > 2 OR (Store.e = 2 AND Store.f < 3)) AND Store.b = 1))');
    });

    it('should support multiple conditions for the same field', () => {
      criteria.where('a', 1);
      criteria.where('a', 2);
      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a = 1 AND Store.a = 2)');

      criteria = new DbCriteria(Store);
      criteria.where('a', [
        criteria.gte(10),
        criteria.lte(100)
      ]);
      result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a >= 10 AND Store.a <= 100)');

      criteria = new DbCriteria(Store);
      criteria.where('a', [
        criteria.lte(10),
        criteria.gte(100)
      ], true);
      result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a <= 10 OR Store.a >= 100)');
    });

    it('should process IN', () => {
      criteria.where('a', criteria.in([1, 2, 3]));

      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a IN (1, 2, 3))');
    });

    it('should process NOT IN', () => {
      criteria.where('a', criteria.nin([1, 2, 3]));

      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a NOT IN (1, 2, 3))');
    });

    it('should process normal operators', () => {
      var operators = _.chain(SqlGenerator.OPERATORS).keys().without('in', 'nin', 'eq').value();

      _.each(operators, (operator) => {
        criteria = new DbCriteria(Store);
        criteria.where('a', criteria[operator](1));

        var result = generator.select(criteria);
        expect(result).toEqual(`SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a ${SqlGenerator.OPERATORS[operator]} 1)`);
      });
    });

    it('should use fields in the criteria', () => {
      criteria.where('a', 1);
      criteria.fields('b', 'c', 'd');

      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.b AS "Store.b", Store.c AS "Store.c", Store.d AS "Store.d" FROM Store WHERE (Store.a = 1)');
    });

    it('should support order', () => {
      criteria.order('name');
      criteria.order('id', -1);

      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store ORDER BY Store.name ASC, Store.id DESC');
    });

    it('should support offset', () => {
      criteria.offset(10);

      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store OFFSET 10');
    });

    it('should support limit', () => {
      criteria.limit(10);

      var result = generator.select(criteria);
      expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store LIMIT 10');
    });

    it('should support nested criteria in where condition', () => {
      criteria.where('a', 1);
      criteria.where('b', new DbCriteria(Product, {where: {name: 'Test'}, fields: ['id']}));

      var result = generator.select(criteria);
      expect(result).toEqual(`SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a = 1 AND Store.b = (SELECT id FROM Product WHERE (name = 'Test')))`);

      criteria.where('b', {
        nin: new DbCriteria(Product, {where: {name: 'Test'}, fields: ['id']})
      });
      result = generator.select(criteria);
      expect(result).toEqual(`SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id" FROM Store WHERE (Store.a = 1 AND Store.b = (SELECT id FROM Product WHERE (name = 'Test')) AND Store.b NOT IN (SELECT id FROM Product WHERE (name = 'Test')))`);
    });
  });

  describe('#insert', () => {
    it('should insert a single row', () => {
      criteria.setAttributes({
        name: 'test'
      });

      var result = generator.insert(criteria);

      expect(result).toEqual(`INSERT INTO Store (name) VALUES ('test')`);
    });

    it('should insert a single row using noQuote option', () => {
      criteria.setAttributes({
        name: 'test',
        b: 'GET_DATE()'
      }, {
        force: true
      });

      var result = generator.insert(criteria, {
        noQuote: ['b']
      });

      expect(result).toEqual(`INSERT INTO Store (name, b) VALUES ('test', GET_DATE())`);
    });

    it('should insert multiple rows', () => {
      criteria.setAttributes([
        {name: 'test1'},
        {name: 'test2'}
      ]);

      var result = generator.insert(criteria);

      expect(result).toEqual(`INSERT INTO Store (name) VALUES ('test1'), ('test2')`);
    });
  });

  describe('#update', () => {
    it('should update a row', () => {
      criteria.setAttributes({
        name: 'test'
      });

      var result = generator.update(criteria);

      expect(result).toEqual(`UPDATE Store SET name = 'test'`);
    });

    it('should update a row using WHERE', () => {
      criteria.where('c', 1);
      criteria.setAttributes({
        name: 'test'
      });

      var result = generator.update(criteria);

      expect(result).toEqual(`UPDATE Store SET name = 'test' WHERE (c = 1)`);
    });

    it('should update a row using noQuote', () => {
      criteria.setAttributes({
        name: 'test',
        b: 'GET_DATE()'
      }, {
        force: true
      });

      var result = generator.update(criteria, {
        noQuote: 'b'
      });

      expect(result).toEqual(`UPDATE Store SET name = 'test', b = GET_DATE()`);
    });
  });

  describe('#delete', () => {
    it('should delete all rows', () => {
      var result = generator.delete(criteria);

      expect(result).toEqual('DELETE FROM Store');
    });

    it('should delete rows using criteria', () => {
      criteria.where('c', 1);
      var result = generator.delete(criteria);

      expect(result).toEqual('DELETE FROM Store WHERE (c = 1)');
    });

    it('should support nested criteria', () => {
      criteria = new DbCriteria(ProductTag);

      criteria.where('product_id', 1);
      criteria.where('tag_id', {
        in: new DbCriteria(Tag, {where: {name: 'Tag1'}, fields: ['id']})
      });
      var result = generator.delete(criteria);

      expect(result).toEqual(`DELETE FROM ProductTag WHERE (product_id = 1 AND tag_id IN (SELECT id FROM Tag WHERE (name = 'Tag1')))`);
    });

  });

  describe('#count', () => {
    it('should count all rows', () => {
      var result = generator.count(criteria);

      expect(result).toEqual('SELECT COUNT(id) FROM Store');
    });

    it('should count rows using criteria', () => {
      criteria.where('c', 1);
      var result = generator.count(criteria);

      expect(result).toEqual('SELECT COUNT(id) FROM Store WHERE (c = 1)');
    });

  });

  describe('psql engine', () => {
    beforeEach(() => {
      generator = new SqlGenerator(SqlGenerator.PSQL);
    });

    describe('#select', () => {
      it('should support include hasMany relations', () => {
        criteria.include('products');

        var result = generator.select(criteria);
        expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id", products.name AS "products.name", products.id AS "products.id", products.store_id AS "products.store_id" FROM Store LEFT JOIN (SELECT name, id, store_id, ROW_NUMBER() OVER (PARTITION BY products.store_id) AS _rn_ FROM Product products) products ON (Store.id = products.store_id)');
      });

      it('should support include hasMany relations with correct order in the main model', () => {
        criteria.include('products');
        criteria.order('name', -1);

        var result = generator.select(criteria);
        expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id", products.name AS "products.name", products.id AS "products.id", products.store_id AS "products.store_id" FROM Store LEFT JOIN (SELECT name, id, store_id, ROW_NUMBER() OVER (PARTITION BY products.store_id) AS _rn_ FROM Product products) products ON (Store.id = products.store_id) ORDER BY Store.name DESC');
      });

      it('should support include hasMany relations with limit', () => {
        criteria.include('products', {
          limit: 1
        });

        var result = generator.select(criteria);
        expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id", products.name AS "products.name", products.id AS "products.id", products.store_id AS "products.store_id", products._rn_ AS "products._rn_" FROM Store LEFT JOIN (SELECT name, id, store_id, ROW_NUMBER() OVER (PARTITION BY products.store_id) AS _rn_ FROM Product products) products ON (Store.id = products.store_id) WHERE (products._rn_ <= 1 OR products._rn_ IS NULL)');
      });

      it('should support include hasMany relations with where conditions', () => {
        criteria.include('products', {
          where: {
            name: {
              neq: 'Product 1'
            }
          }
        });

        var result = generator.select(criteria);
        expect(result).toEqual(`SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id", products.name AS "products.name", products.id AS "products.id", products.store_id AS "products.store_id" FROM Store LEFT JOIN (SELECT name, id, store_id, ROW_NUMBER() OVER (PARTITION BY products.store_id) AS _rn_ FROM Product products WHERE (name <> 'Product 1')) products ON (Store.id = products.store_id)`);
      });

      it('should support include hasMany relations with order', () => {
        criteria.include('products', {
          order: {
            name: -1
          },
          limit: 1
        });

        var result = generator.select(criteria);
        expect(result).toEqual(`SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id", products.name AS "products.name", products.id AS "products.id", products.store_id AS "products.store_id", products._rn_ AS "products._rn_" FROM Store LEFT JOIN (SELECT name, id, store_id, ROW_NUMBER() OVER (PARTITION BY products.store_id ORDER BY name DESC) AS _rn_ FROM Product products) products ON (Store.id = products.store_id) WHERE (products._rn_ <= 1 OR products._rn_ IS NULL)`);
      });

      it('should support include belongsTo relations', () => {
        criteria.include('owner');

        var result = generator.select(criteria);
        expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id", owner.name AS "owner.name", owner.id AS "owner.id" FROM Store LEFT JOIN (SELECT name, id FROM Users owner) owner ON (Store.user_id = owner.id)');
      });

      it('should ignore where conditions for belongsTo relations', () => {
        criteria.include('owner', {
          where: {
            name: 'Test'
          }
        });

        var result = generator.select(criteria);
        expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id", owner.name AS "owner.name", owner.id AS "owner.id" FROM Store LEFT JOIN (SELECT name, id FROM Users owner) owner ON (Store.user_id = owner.id)');
      });

      it('should ignore limit for belongsTo relations', () => {
        criteria.include('owner', {
          limit: 1
        });

        var result = generator.select(criteria);
        expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id", owner.name AS "owner.name", owner.id AS "owner.id" FROM Store LEFT JOIN (SELECT name, id FROM Users owner) owner ON (Store.user_id = owner.id)');
      });

      it('should ignore order for belongsTo relations', () => {
        criteria.include('owner', {
          order: {
            name: -1
          }
        });

        var result = generator.select(criteria);
        expect(result).toEqual('SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id", owner.name AS "owner.name", owner.id AS "owner.id" FROM Store LEFT JOIN (SELECT name, id FROM Users owner) owner ON (Store.user_id = owner.id)');
      });

      it('should support nested relations', () => {
        criteria.include('owner', {
          include: 'comments'
        });

        var result = generator.select(criteria);
        expect(result).toEqual(`SELECT Store.name AS "Store.name", Store.id AS "Store.id", Store.user_id AS "Store.user_id", owner.name AS "owner.name", owner.id AS "owner.id", owner$comments.content AS "owner$comments.content", owner$comments.id AS "owner$comments.id", owner$comments.user_id AS "owner$comments.user_id", owner$comments.product_id AS "owner$comments.product_id" FROM Store LEFT JOIN (SELECT name, id FROM Users owner) owner ON (Store.user_id = owner.id) LEFT JOIN (SELECT content, id, user_id, product_id, ROW_NUMBER() OVER (PARTITION BY comments.user_id) AS _rn_ FROM Comment comments) owner$comments ON (owner.id = owner$comments.user_id)`);
      });

      it('should include many-to-many relations', () => {
        criteria = new DbCriteria(Product);
        criteria.include('tags');

        var result = generator.select(criteria);
        expect(result).toEqual(`SELECT Product.name AS "Product.name", Product.id AS "Product.id", Product.store_id AS "Product.store_id", tags.name AS "tags.name", tags.id AS "tags.id" FROM Product LEFT JOIN (SELECT Tag.name, Tag.id, tags.tag_id, tags.product_id, ROW_NUMBER() OVER (PARTITION BY tags.product_id) AS _rn_ FROM ProductTag tags LEFT JOIN Tag ON (tags.tag_id = Tag.id)) tags ON (Product.id = tags.product_id)`);
      });

      it('should include many-to-many relations with where conditions', () => {
        criteria = new DbCriteria(Product);
        criteria.include('tags', {
          where: {
            name: 'Tag1'
          }
        });

        var result = generator.select(criteria);
        expect(result).toEqual(`SELECT Product.name AS "Product.name", Product.id AS "Product.id", Product.store_id AS "Product.store_id", tags.name AS "tags.name", tags.id AS "tags.id" FROM Product LEFT JOIN (SELECT Tag.name, Tag.id, tags.tag_id, tags.product_id, ROW_NUMBER() OVER (PARTITION BY tags.product_id) AS _rn_ FROM ProductTag tags LEFT JOIN Tag ON (tags.tag_id = Tag.id) WHERE (Tag.name = 'Tag1')) tags ON (Product.id = tags.product_id)`);
      });

      it('should include many-to-many relations with limit', () => {
        criteria = new DbCriteria(Product);
        criteria.include('tags', {
          limit: 1
        });

        var result = generator.select(criteria);
        expect(result).toEqual(`SELECT Product.name AS "Product.name", Product.id AS "Product.id", Product.store_id AS "Product.store_id", tags.name AS "tags.name", tags.id AS "tags.id", tags._rn_ AS "tags._rn_" FROM Product LEFT JOIN (SELECT Tag.name, Tag.id, tags.tag_id, tags.product_id, ROW_NUMBER() OVER (PARTITION BY tags.product_id) AS _rn_ FROM ProductTag tags LEFT JOIN Tag ON (tags.tag_id = Tag.id)) tags ON (Product.id = tags.product_id) WHERE (tags._rn_ <= 1 OR tags._rn_ IS NULL)`);
      });

      it('should include many-to-many relations with order', () => {
        criteria = new DbCriteria(Product);
        criteria.include('tags', {
          order: {
            name: 1
          },
          limit: 1
        });

        var result = generator.select(criteria);
        expect(result).toEqual(`SELECT Product.name AS "Product.name", Product.id AS "Product.id", Product.store_id AS "Product.store_id", tags.name AS "tags.name", tags.id AS "tags.id", tags._rn_ AS "tags._rn_" FROM Product LEFT JOIN (SELECT Tag.name, Tag.id, tags.tag_id, tags.product_id, ROW_NUMBER() OVER (PARTITION BY tags.product_id ORDER BY Tag.name ASC) AS _rn_ FROM ProductTag tags LEFT JOIN Tag ON (tags.tag_id = Tag.id)) tags ON (Product.id = tags.product_id) WHERE (tags._rn_ <= 1 OR tags._rn_ IS NULL)`);
      });
    });

    describe('#insert', () => {
      it('should return all fields after inserting', () => {
        criteria.setAttributes({
          name: 'test'
        });
        var result = generator.insert(criteria);

        expect(result).toEqual(`INSERT INTO Store (name) VALUES ('test') RETURNING *`);
      });
    });

    describe('#update', () => {
      it('should return all fields after updating', () => {
        criteria.setAttributes({
          name: 'test'
        });
        criteria.where({
          c: 1
        });
        var result = generator.update(criteria);

        expect(result).toEqual(`UPDATE Store SET name = 'test' WHERE (c = 1) RETURNING *`);
      });
    });

    describe('#delete', () => {
      it('should return all fields after deleting', () => {
        criteria.where('c', 1);
        var result = generator.delete(criteria);

        expect(result).toEqual('DELETE FROM Store WHERE (c = 1) RETURNING *');
      });

    });
  });

});
