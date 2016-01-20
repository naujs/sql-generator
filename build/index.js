'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var DbCriteria = require('@naujs/db-criteria'),
    _ = require('lodash'),
    squel = require('squel');

function checkCriteria(criteria) {
  if (criteria instanceof DbCriteria) {
    return criteria;
  } else if (_.isObject(criteria)) {
    return new DbCriteria(criteria);
  } else if (criteria === void 0 || criteria === null) {
    return new DbCriteria();
  }

  throw 'Invalid criteria';
}

var PSQL = 'psql';

var OPERATORS = {
  'eq': '=',
  'neq': '<>',
  'gt': '>',
  'gte': '>=',
  'lt': '<',
  'lte': '<=',
  'in': 'IN',
  'nin': 'NOT IN'
};

function generateCriteria(stm, criteria) {
  // stm can be select, update or delete query
  var where = generateWhereStatment(criteria.getWhere());
  stm = stm.where(where.toString());

  var fields = criteria.getFields();
  if (fields && fields.length) {
    _.each(fields, function (field) {
      stm = stm.field(field);
    });
  }

  _.each(criteria.getOrder(), function (direction, key) {
    stm = stm.order(key, direction);
  });

  // update doesnt have offset
  if (stm.offset) {
    stm = stm.offset(criteria.getOffset());
  }

  var limit = criteria.getLimit();
  if (limit) {
    stm = stm.limit(limit);
  }

  return stm;
}

function generateWhereStatment(where, expr) {
  if (!where || !where.length) {
    return '';
  }

  expr = expr || squel.expr();

  _.each(where, function (condition) {
    if (condition.where) {
      if (condition.or) {
        expr = expr.or_begin();
      } else {
        expr = expr.and_begin();
      }

      expr = generateWhereStatment(condition.where, expr);
      expr = expr.end();
    } else {
      var stm = [condition.key, OPERATORS[condition.operator], '?'].join(' ');
      if (condition.or) {
        expr = expr.or(stm, condition.value);
      } else {
        expr = expr.and(stm, condition.value);
      }
    }
  });

  return expr;
}

function generateSet(insertOrUpdate, attributes) {
  var noQuote = arguments.length <= 2 || arguments[2] === undefined ? [] : arguments[2];

  // noQuote contains a list of attributes that should not be quoted.
  // This is useful when using native functions as the value

  _.each(attributes, function (v, k) {
    var opts = {};

    if (_.indexOf(noQuote, k) !== -1) {
      opts.dontQuote = true;
    }

    insertOrUpdate = insertOrUpdate.set(k, v, opts);
  });

  return insertOrUpdate;
}

function initSquelForSpecificEngine(engine) {
  if (!engine) {
    return squel;
  }

  switch (engine) {
    case PSQL:
      return squel.useFlavour('postgres');
    default:
      return squel;
  }
}

function processEngineSpecificInsertQuery(insert, engine) {
  if (!engine) {
    return insert;
  }

  switch (engine) {
    case PSQL:
      return insert.returning('*');
    default:
      return insert;
  }
}

function processEngineSpecificUpdateQuery(update, engine) {
  if (!engine) {
    return update;
  }

  switch (engine) {
    case PSQL:
      return update.returning('*');
    default:
      return update;
  }
}

var Generator = function () {
  function Generator(engine) {
    _classCallCheck(this, Generator);

    this._engine = engine ? engine.toLowerCase() : null;
    this._squel = initSquelForSpecificEngine(this._engine);
  }

  _createClass(Generator, [{
    key: 'select',
    value: function select(tableName, criteria) {
      criteria = checkCriteria(criteria);

      var select = this._squel.select().from(tableName);

      select = generateCriteria(select, criteria);

      return select.toString();
    }
  }, {
    key: 'insert',
    value: function insert(tableName, attributes) {
      var options = arguments.length <= 2 || arguments[2] === undefined ? {} : arguments[2];

      if (!_.isArray(attributes) && !_.isObject(attributes) || !_.size(attributes)) {
        throw 'Invalid param';
      }

      var insert = this._squel.insert().into(tableName);

      if (_.isArray(attributes)) {
        // Unfortunately, it is not possible to use native functions
        // when doing bulk insert
        insert.setFieldsRows(attributes);
      } else {
        insert = generateSet(insert, attributes, options.noQuote);
      }

      insert = processEngineSpecificInsertQuery(insert, this._engine);

      return insert.toString();
    }
  }, {
    key: 'update',
    value: function update(tableName, criteria, attributes) {
      var options = arguments.length <= 3 || arguments[3] === undefined ? {} : arguments[3];

      criteria = checkCriteria(criteria);
      if (!_.isObject(attributes) || !_.size(attributes)) {
        throw 'Invalid param';
      }

      var update = this._squel.update().table(tableName);
      update = generateSet(update, attributes, options.noQuote);
      update = generateCriteria(update, criteria);

      update = processEngineSpecificUpdateQuery(update, this._engine);

      return update.toString();
    }
  }, {
    key: 'delete',
    value: function _delete(tableName, criteria) {
      criteria = checkCriteria(criteria);

      var del = this._squel.delete().from(tableName);
      del = generateCriteria(del, criteria);

      return del.toString();
    }
  }]);

  return Generator;
}();

Generator.OPERATORS = OPERATORS;
Generator.PSQL = PSQL;

module.exports = Generator;