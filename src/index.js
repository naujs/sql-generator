'use strict';

var DbCriteria = require('@naujs/db-criteria')
  , _ = require('lodash')
  , squel = require('squel');

function checkCriteria(criteria) {
  if (criteria instanceof DbCriteria) {
    return criteria;
  } else if (_.isObject(criteria)) {
    return new DbCriteria(criteria);
  } else if (criteria === void(0) || criteria === null) {
    return new DbCriteria();
  }

  throw 'Invalid criteria';
}

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

function generateSelect(selectOrUpdate, criteria) {
  selectOrUpdate = selectOrUpdate.where(generateWhereStatment(criteria.getWhere()));
  var fields = criteria.getFields();
  if (fields && fields.length) {
    _.each(fields, (field) => {
      selectOrUpdate = selectOrUpdate.field(field);
    });
  }

  _.each(criteria.getOrder(), (direction, key) => {
    selectOrUpdate = selectOrUpdate.order(key, direction);
  });

  // update doesnt have offset
  if (selectOrUpdate.offset) {
    selectOrUpdate = selectOrUpdate.offset(criteria.getOffset());
  }

  var limit = criteria.getLimit();
  if (limit) {
    selectOrUpdate = selectOrUpdate.limit(limit);
  }

  return selectOrUpdate;
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

class Generator {
  select(tableName, criteria) {
    criteria = checkCriteria(criteria);

    var select = squel.select()
                      .from(tableName);

    select = generateSelect(select, criteria);

    return select.toString();
  }

  insert(tableName, attributes, options = {}) {
    if ((!_.isArray(attributes) && !_.isObject(attributes)) || !_.size(attributes)) {
      throw 'Invalid param';
    }

    var insert = squel.insert().into(tableName);

    if (_.isArray(attributes)) {
      // Unfortunately, it is not possible to use native functions
      // when doing bulk insert
      insert.setFieldsRows(attributes);
    } else {
      insert = generateSet(insert, attributes, options.noQuote);
    }

    return insert.toString();
  }

  update(tableName, attributes, criteria, options = {}) {
    criteria = checkCriteria(criteria);

    if (!_.isObject(attributes) || !_.size(attributes)) {
      throw 'Invalid param';
    }

    var update = squel.update().table(tableName);
    update = generateSet(update, attributes, options.noQuote);
    update = generateSelect(update, criteria);

    return update.toString();
  }
}

Generator.OPERATORS = OPERATORS;

module.exports = Generator;
