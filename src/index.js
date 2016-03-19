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

function getAllPropertiesFromMeta(meta) {
  var properties = _.chain(meta.properties).clone().keys().value();
  properties.push(meta.primaryKey);
  var foreignKeys = _.chain(meta.relations).map((relation) => {
    // TODO: use constants here
    if (relation.type == 'belongsTo') {
      return relation.foreignKey;
    }
    return null;
  }).compact().value();
  return _.union(properties, foreignKeys);
}

function generateCriteria(type, stm, criteria, meta) {
  // type can be `select`, `update` or `delete`
  var where = generateWhereStatment(criteria.getWhere(), type == 'select' ? meta.modelName : null);
  stm = stm.where(where.toString());
  var tableName = meta.modelName;

  var fields = criteria.getFields();
  if (fields && fields.length && type == 'select') {
    _.each(fields, (field) => {
      var name = `${tableName}.${field}`;
      stm = stm.field(name, name);
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

function generateWhereStatment(where, alias, expr) {
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

      expr = generateWhereStatment(condition.where, alias, expr);
      expr = expr.end();
    } else {
      var key = condition.key;
      if (alias) {
        key = `${alias}.${condition.key}`;
      }
      var stm = [key, OPERATORS[condition.operator], '?'].join(' ');
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

function processEngineSpecificDeleteQuery(del, engine) {
  if (!engine) {
    return del;
  }

  switch(engine) {
    case PSQL:
      return del.returning('*');
    default:
      return del;
  }
}

function processEngineSpecificJoinQuery(select, engine, include, meta) {
  return select;
}

class Generator {
  constructor(engine) {
    this._engine = engine ? engine.toLowerCase() : null;
    this._squel = initSquelForSpecificEngine(this._engine);
  }

  select(filter, meta, options = {}) {
    var criteria = checkCriteria(filter);

    var select = this._squel.select()
                      .from(meta.modelName);

    // always explicitly specify fields
    var fields = criteria.getFields();
    if (!fields || !fields.length) {
      var properties = getAllPropertiesFromMeta(meta);
      criteria.fields(...properties);
    }

    select = generateCriteria('select', select, criteria, meta);

    var include = criteria.getInclude();
    if (include && include.length) {
      select = processEngineSpecificJoinQuery(select, this._engine, include, meta);
    }

    return select.toString();
  }

  insert(attributes, meta, options = {}) {
    if ((!_.isArray(attributes) && !_.isObject(attributes)) || !_.size(attributes)) {
      throw 'Invalid param';
    }

    var insert = this._squel.insert().into(meta.modelName);

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

  update(filter, attributes, meta, options = {}) {
    var criteria = checkCriteria(filter);
    if (!_.isObject(attributes) || !_.size(attributes)) {
      throw 'Invalid param';
    }

    var update = this._squel.update().table(meta.modelName);
    update = generateSet(update, attributes, options.noQuote);
    update = generateCriteria('update', update, criteria, meta);

    update = processEngineSpecificUpdateQuery(update, this._engine);

    return update.toString();
  }

  delete(filter, meta, options = {}) {
    var criteria = checkCriteria(filter);

    var del = this._squel.delete().from(meta.modelName);
    del = generateCriteria('delete', del, criteria, meta);

    del = processEngineSpecificDeleteQuery(del, this._engine);

    return del.toString();
  }

  count(filter, meta, options = {}) {
    var criteria = checkCriteria(filter);
    var where = generateWhereStatment(criteria.getWhere());

    var select = this._squel.select()
                            .from(meta.modelName)
                            .field('COUNT(' + meta.primaryKey + ')')
                            .where(where.toString());

    return select.toString();
  }
}

Generator.OPERATORS = OPERATORS;
Generator.PSQL = PSQL;

module.exports = Generator;
