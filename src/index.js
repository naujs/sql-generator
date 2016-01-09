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

class Generator {
  select(tableName, criteria) {
    criteria = checkCriteria(criteria);

    var select = squel.select()
                      .from(tableName)
                      .where(generateWhereStatment(criteria.getWhere()));

    var fields = criteria.getFields();
    if (fields && fields.length) {
      _.each(fields, (field) => {
        select = select.field(field);
      });
    }

    _.each(criteria.getOrder(), (direction, key) => {
      select = select.order(key, direction);
    });

    select = select.offset(criteria.getOffset());
    var limit = criteria.getLimit();
    if (limit) {
      select = select.limit(limit);
    }

    return select.toString();
  }

  insert(tableName, attributes, options = {}) {
    if ((!_.isArray(attributes) && !_.isObject(attributes)) || !_.size(attributes)) {
      throw 'Invalid param';
    }

    // Contains a list of attributes that should not be quoted.
    // This is useful when using native functions as the value
    var noQuote = options.noQuote || [];

    var insert = squel.insert().into(tableName);

    if (_.isArray(attributes)) {
      // Unfortunately, it is not possible to use native functions
      // when doing bulk insert
      insert.setFieldsRows(attributes);
    } else {
      _.each(attributes, (v, k) => {
        let opts = {};

        if (_.indexOf(noQuote, k) !== -1) {
          opts.dontQuote = true;
        }

        insert.set(k, v, opts);
      });
    }

    return insert.toString();
  }

  update(tableName, attributes, criteria) {
    criteria = checkCriteria(criteria);
  }
}

Generator.OPERATORS = OPERATORS;

module.exports = Generator;
