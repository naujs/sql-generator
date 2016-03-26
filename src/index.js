'use strict';

// TODO: Implement OFFSET for JOIN queries

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

const ROW_NUMBER_COL = '_rn_';
const SELECT_OPTIONS = {
  autoQuoteAliasNames: false // manually quote alias
};

function generateCriteria(type, stm, criteria) {
  var Model = criteria.getModelClass();
  var modelName = Model.getModelName();
  // type can be `select`, `update` or `delete`
  // We only need alias for select queries to make it consistent with queries
  // using JOIN
  var whereAlias = type == 'select' ? modelName : null;
  var where = generateWhereStatement(criteria.getWhere(), whereAlias);
  stm = stm.where(where.toString());
  var tableName = modelName;

  var fields = criteria.getFields();
  if (fields && fields.length && type == 'select') {
    _.each(fields, (field) => {
      var name = `${tableName}.${field}`;
      stm = stm.field(name, `"${name}"`);
    });
  }

  stm = generateOrder(stm, criteria);

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

function generateOrder(stm, criteria) {
  var Model = criteria.getModelClass();
  var modelName = Model.getModelName();

  _.each(criteria.getOrder(), (direction, key) => {
    stm = stm.order(`${modelName}.${key}`, direction === 1);
  });
  return stm;
}

function generateWhereStatement(where, alias, expr) {
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

      expr = generateWhereStatement(condition.where, alias, expr);
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

function generateSubSelectForLeftJoinQuery(squel, include) {
  var subSelect;
  switch (include.type) {
    case 'hasMany':
    case 'hasOne':
    case 'belongsTo':
      // For these types, generate a normal sub SELECT query
      subSelect = squel.select(SELECT_OPTIONS).from(include.target.modelName, include.relation);
      for (let j in include.target.properties) {
        let prop = include.target.properties[j];
        subSelect.field(`${prop}`);
      }
      break;
    case 'hasManyAndBelongsTo':
      subSelect = squel.select(SELECT_OPTIONS).from(include.through.modelName, include.relation);
      for (let j in include.target.properties) {
        let prop = include.target.properties[j];
        // Include all the fields from the joined table
        subSelect.field(`${include.target.modelName}.${prop}`);
      }
      // also include 2 fields in the junction table to perform ON
      subSelect.field(`${include.relation}.${include.target.foreignKey}`);
      subSelect.field(`${include.relation}.${include.through.foreignKey}`);
      break;
  }
  return subSelect;
}

function generateWhereForSubSelectInLeftJoinQuery(subSelect, include) {
  var criteria = include.target.criteria;
  if (!criteria) return subSelect;

  var where = criteria.getWhere();
  if (!where.length) return subSelect;

  var alias = null;

  switch (include.type) {
    case 'belongsTo':
      // It doesn't make any sense to perform where conditions for belongsTo
      return subSelect;
    case 'hasManyAndBelongsTo':
      // In case of many-to-many, we need to refer to the main table to
      // perform where condition because the junction table does not have anything
      // For other cases, it is the other way around
      alias = include.target.modelName;
      break;
  }

  where = generateWhereStatement(where, alias);
  subSelect.where(where.toString());
  return subSelect;
}

function generatePsqlLeftJoinQuery(squel, criteria, cb, parentRelation) {
  var queries = [];
  var includeParams = criteria.getInclude();
  // Use the parentRelation as main alias when available
  var modelName = parentRelation ? parentRelation : criteria.getModelClass().getModelName();
  for (var i in includeParams) {
    var include = includeParams[i];

    // Set the alias for the join table using parentRelation when available
    var joinAlias = parentRelation ? `${parentRelation}$${include.relation}` : include.relation;
    var subCriteria = include.target.criteria;

    // Notify the outer function so that it can generate select field
    cb(include, parentRelation);

    var subSelect = generateSubSelectForLeftJoinQuery(squel, include);
    subSelect = generateWhereForSubSelectInLeftJoinQuery(subSelect, include);

    var onCondition = null;
    var partitionBy = null;
    var orderBy = [];
    var order = subCriteria ? subCriteria.getOrder() : {};
    switch (include.type) {
      case 'hasMany':
      case 'hasOne':
        onCondition = `${modelName}.${include.target.referenceKey} = ${joinAlias}.${include.target.foreignKey}`;
        partitionBy = `${include.relation}.${include.target.foreignKey}`;
        // Generate order statements
        for (let field in order) {
          let direction = order[field];
          orderBy.push(`${field} ${direction === 1 ? 'ASC' : 'DESC'}`);
        }
        break;
      case 'belongsTo':
        // In this case, just JOIN 2 tables without any special sub query
        onCondition = `${modelName}.${include.target.foreignKey} = ${joinAlias}.${include.target.referenceKey}`;
        break;
      case 'hasManyAndBelongsTo':
        // For many-to-many, we need to have another LEFT JOIN to join
        // the other table in order to get correct fields
        // Because the junction table usually does not have any data
        onCondition = `${modelName}.${include.through.referenceKey} = ${joinAlias}.${include.through.foreignKey}`;
        partitionBy = `${include.relation}.${include.through.foreignKey}`;
        subSelect.left_join(include.target.modelName, null, `${include.relation}.${include.target.foreignKey} = ${include.target.modelName}.${include.target.referenceKey}`);
        // Generate order statements
        for (let field in order) {
          let direction = order[field];
          orderBy.push(`${include.target.modelName}.${field} ${direction === 1 ? 'ASC' : 'DESC'}`);
        }
        break;
    }

    orderBy = orderBy.join(', ');
    if (orderBy) {
      orderBy = ' ORDER BY ' + orderBy;
    }

    if (partitionBy) {
      // For each rows returned, set the row number so that we can do limit later
      subSelect.field(`ROW_NUMBER() OVER (PARTITION BY ${partitionBy}${orderBy})`, ROW_NUMBER_COL);
    }

    queries.push([
      subSelect,
      joinAlias,
      onCondition
    ]);

    // Recursively do all the nested criteria
    if (subCriteria) {
      var nestedInclude = subCriteria.getInclude();
      if (nestedInclude && nestedInclude.length) {
        queries = queries.concat(generatePsqlLeftJoinQuery(squel, subCriteria, cb, joinAlias));
      }
    }
  }

  return queries;
}

function processEngineSpecificJoinQuery(squel, engine, criteria) {
  var modelName = criteria.getModelClass().getModelName();
  var select = squel.select(SELECT_OPTIONS).from(modelName);

  // Explicitly select fields from the main model
  var fields = criteria.getFields();
  if (!fields || !fields.length) {
    fields = criteria.getModelClass().getAllProperties();
  }

  _.each(fields, (field) => {
    select.field(`${modelName}.${field}`, `"${modelName}.${field}"`);
  });

  switch(engine) {
    case PSQL:
      var queries = generatePsqlLeftJoinQuery(squel, criteria, function(include, relation) {
        // The naming convention is to use $ to separate relation and its relation
        // For example: include('products', {include: 'comments'}) results in
        // products$comments
        var prefix = relation ? `${relation}$${include.relation}` : `${include.relation}`;

        // For each include param, select all the fields
        for (var j in include.target.properties) {
          var prop = include.target.properties[j];
          var name = `${prefix}.${prop}`;
          select.field(name, `"${name}"`);
        }

        // Apply limit based on the row number (rn)
        var subCriteria = include.target.criteria;
        if (subCriteria) {
          var limit = subCriteria.getLimit();
          // belongsTo relation indicates that there should be zero or one related row
          if (limit && include.type != 'belongsTo') {
            var rn = `${prefix}.${ROW_NUMBER_COL}`;
            select.field(rn, `"${rn}"`);
            select.where(`${rn} <= ${limit} OR ${rn} IS NULL`);
          }
        }
      });

      _.each(queries, (q) => {
        select.left_join(...q);
      });

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
    // JOIN queries are special, therefore they are built separately
    if (include && include.length && this._engine) {
      select = processEngineSpecificJoinQuery(this._squel, this._engine, criteria);
      if (!select) {
        console.warn(`${this._engine} does not support include`);
        return null;
      }
    } else {
      select = this._squel.select(SELECT_OPTIONS).from(modelName);
      // always explicitly specify fields
      var fields = criteria.getFields();
      if (!fields || !fields.length) {
        var properties = criteria.getModelClass().getAllProperties();
        criteria.fields(...properties);
      }
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
    var where = generateWhereStatement(criteria.getWhere());

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
