'use strict';

var DbCriteria = require('@naujs/db-criteria')
  , _ = require('lodash')
  , squel = require('squel');

function checkCriteria(criteria) {
  if (criteria instanceof DbCriteria) {
    return criteria;
  } else if (_.isObject(criteria)) {
    return new DbCriteria({where: criteria});
  } else if (criteria === void(0) || criteria === null) {
    return new DbCriteria();
  }

  throw 'Invalid criteria';
}

const PSQL = 'psql';

const OPERATORS = {
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
    _.each(fields, (field) => {
      stm = stm.field(field);
    });
  }

  _.each(criteria.getOrder(), (direction, key) => {
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

  _.each(where, (condition) => {
    if (condition.where) {
      if (condition.or) {
        expr = expr.or_begin();
      } else {
        expr = expr.and_begin();
      }

      expr = generateWhereStatment(condition.where, expr);
      expr = expr.end();
    } else {
      let stm = [condition.key, OPERATORS[condition.operator], '?'].join(' ');
      if (condition.or) {
        expr = expr.or(stm, condition.value);
      } else {
        expr = expr.and(stm, condition.value);
      }
    }
  });

  return expr;
}

function generateSet(insertOrUpdate, attributes, noQuote = []) {
  // noQuote contains a list of attributes that should not be quoted.
  // This is useful when using native functions as the value

  _.each(attributes, (v, k) => {
    let opts = {};

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

  switch(engine) {
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

  switch(engine) {
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

  switch(engine) {
    case PSQL:
      return update.returning('*');
    default:
      return update;
  }
}

class Generator {
  constructor(engine) {
    this._engine = engine ? engine.toLowerCase() : null;
    this._squel = initSquelForSpecificEngine(this._engine);
  }

  select(tableName, criteria) {
    criteria = checkCriteria(criteria);

    var select = this._squel.select()
                      .from(tableName);

    select = generateCriteria(select, criteria);

    return select.toString();
  }

  insert(tableName, attributes, options = {}) {
    if ((!_.isArray(attributes) && !_.isObject(attributes)) || !_.size(attributes)) {
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

  update(tableName, criteria, attributes, options = {}) {
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

  delete(tableName, criteria) {
    criteria = checkCriteria(criteria);

    var del = this._squel.delete().from(tableName);
    del = generateCriteria(del, criteria);

    return del.toString();
  }
}

Generator.OPERATORS = OPERATORS;
Generator.PSQL = PSQL;

module.exports = Generator;
