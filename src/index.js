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

function generateCriteria(type, stm, criteria) {
  var Model = criteria.getModelClass();
  var modelName = Model.getModelName();
  // type can be `select`, `update` or `delete`
  var where = generateWhereStatment(criteria.getWhere(), type == 'select' ? modelName : null);
  stm = stm.where(where.toString());
  var tableName = modelName;

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

function convertIncludeIntoJoinQueryData(include, meta) {
  var data = [];
  for (var i of include) {
    var includeRelation = include[i];
    var relationName = includeRelation.relation;
    var relation = meta.relations[relationName];

    if (relation.type == 'belongsToAndHasMany') {

    }

    var d = {
      from: relationMeta.modelName,
      primary: {
        to: meta.modelName,
        join: [relationMeta.foreignKey, meta.referenceKey]
      }
    };
  }
}

function processEngineSpecificJoinQuery(squel, engine, criteria, meta) {
  switch(engine) {
    case PSQL:
      var mainSelect = squel.select().from('__result__');
      var include = criteria.getInclude();
      return select;
    default:
      return null;
  }
}

class Generator {
  constructor(engine) {
    this._engine = engine ? engine.toLowerCase() : null;
    this._squel = initSquelForSpecificEngine(this._engine);
  }

  select(criteria, options = {}) {
    var include = criteria.getInclude();
    var modelName = criteria.getModelClass().getModelName();
    var select;
    if (include && include.length && this._engine) {
      select = processEngineSpecificJoinQuery(this._squel, this._engine, criteria);
      if (!select) {
        console.warn(`${this._engine} does not support include`);
      }
      return select.toString();
    }

    select = this._squel.select().from(modelName);

    // always explicitly specify fields
    var fields = criteria.getFields();
    if (!fields || !fields.length) {
      var properties = criteria.getModelClass().getAllProperties();
      criteria.fields(...properties);
    }

    select = generateCriteria('select', select, criteria);

    return select.toString();
  }

  insert(criteria, options = {}) {
    var attributes = criteria.getAttributes();
    var modelName = criteria.getModelClass().getModelName();
    if ((!_.isArray(attributes) && !_.isObject(attributes)) || !_.size(attributes)) {
      throw 'Invalid param';
    }

    var insert = this._squel.insert().into(modelName);

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

  update(criteria, options = {}) {
    var attributes = criteria.getAttributes();
    var modelName = criteria.getModelClass().getModelName();
    if (!_.isObject(attributes) || !_.size(attributes)) {
      throw 'Invalid param';
    }

    var update = this._squel.update().table(modelName);
    update = generateSet(update, attributes, options.noQuote);
    update = generateCriteria('update', update, criteria);

    update = processEngineSpecificUpdateQuery(update, this._engine);

    return update.toString();
  }

  delete(criteria, options = {}) {
    var modelName = criteria.getModelClass().getModelName();
    var del = this._squel.delete().from(modelName);
    del = generateCriteria('delete', del, criteria);

    del = processEngineSpecificDeleteQuery(del, this._engine);

    return del.toString();
  }

  count(criteria, options = {}) {
    var modelName = criteria.getModelClass().getModelName();
    var primaryKey = criteria.getModelClass().getPrimaryKey();
    var where = generateWhereStatment(criteria.getWhere());

    var select = this._squel.select()
                            .from(modelName)
                            .field('COUNT(' + primaryKey + ')')
                            .where(where.toString());

    return select.toString();
  }
}

Generator.OPERATORS = OPERATORS;
Generator.PSQL = PSQL;

module.exports = Generator;
