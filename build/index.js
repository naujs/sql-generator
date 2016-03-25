'use strict';

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } else { return Array.from(arr); } }

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

function generateCriteria(type, stm, criteria) {
  var Model = criteria.getModelClass();
  var modelName = Model.getModelName();
  // type can be `select`, `update` or `delete`
  var where = generateWhereStatement(criteria.getWhere(), type == 'select' ? modelName : null);
  stm = stm.where(where.toString());
  var tableName = modelName;

  var fields = criteria.getFields();
  if (fields && fields.length && type == 'select') {
    _.each(fields, function (field) {
      var name = tableName + '.' + field;
      stm = stm.field(name, '"' + name + '"');
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

function generateWhereStatement(where, alias, expr) {
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

      expr = generateWhereStatement(condition.where, alias, expr);
      expr = expr.end();
    } else {
      var key = condition.key;
      if (alias) {
        key = alias + '.' + condition.key;
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

function processEngineSpecificDeleteQuery(del, engine) {
  if (!engine) {
    return del;
  }

  switch (engine) {
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
      subSelect = squel.select().from(include.target.modelName, include.relation);
      for (var j in include.target.properties) {
        var prop = include.target.properties[j];
        subSelect.field('' + prop);
      }
      break;
    case 'hasManyAndBelongsTo':
      subSelect = squel.select().from(include.through.modelName, include.relation);
      for (var j in include.target.properties) {
        var prop = include.target.properties[j];
        subSelect.field(include.target.modelName + '.' + prop);
      }
      subSelect.field(include.relation + '.' + include.target.foreignKey);
      subSelect.field(include.relation + '.' + include.through.foreignKey);
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
    case 'hasManyAndBelongsTo':
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
  var modelName = parentRelation ? parentRelation : criteria.getModelClass().getModelName();
  for (var i in includeParams) {
    var include = includeParams[i];
    var joinAlias = parentRelation ? parentRelation + '$' + include.relation : include.relation;
    var subCriteria = include.target.criteria;

    cb(include, parentRelation);

    var subSelect = generateSubSelectForLeftJoinQuery(squel, include);
    subSelect = generateWhereForSubSelectInLeftJoinQuery(subSelect, include);

    var onCondition, partitionBy;
    switch (include.type) {
      case 'hasMany':
      case 'hasOne':
        onCondition = modelName + '.' + include.target.referenceKey + ' = ' + joinAlias + '.' + include.target.foreignKey;
        partitionBy = include.relation + '.' + include.target.foreignKey;
        break;
      case 'belongsTo':
        onCondition = modelName + '.' + include.target.foreignKey + ' = ' + joinAlias + '.' + include.target.referenceKey;
        break;
      case 'hasManyAndBelongsTo':
        onCondition = modelName + '.' + include.through.referenceKey + ' = ' + joinAlias + '.' + include.through.foreignKey;
        partitionBy = include.relation + '.' + include.through.foreignKey;
        subSelect.left_join(include.target.modelName, null, include.relation + '.' + include.target.foreignKey + ' = ' + include.target.modelName + '.' + include.target.referenceKey);
        break;
    }

    if (partitionBy) {
      subSelect.field('ROW_NUMBER() OVER (PARTITION BY ' + partitionBy + ')', 'rn');
    }

    queries.push([subSelect, joinAlias, onCondition]);

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
  var selectOpts = {
    autoQuoteTableNames: true,
    autoQuoteFieldNames: true
  };
  var modelName = criteria.getModelClass().getModelName();
  var select = squel.select().from(modelName);
  var includeParams = criteria.getInclude();
  var fields = criteria.getFields();
  if (!fields || !fields.length) {
    fields = criteria.getModelClass().getAllProperties();
  }

  _.each(fields, function (field) {
    select.field(modelName + '.' + field, '"' + modelName + '.' + field + '"');
  });

  switch (engine) {
    case PSQL:
      var queries = generatePsqlLeftJoinQuery(squel, criteria, function (include, relation) {
        var prefix = relation ? relation + '$' + include.relation : '' + include.relation;

        for (var j in include.target.properties) {
          var prop = include.target.properties[j];
          var name = prefix + '.' + prop;
          select.field(name, '"' + name + '"');
        }

        var subCriteria = include.target.criteria;
        if (subCriteria) {
          var limit = subCriteria.getLimit();
          if (limit) {
            var rn = prefix + '.rn';
            select.field(rn, '"' + rn + '"');
            select.where(rn + ' <= ' + limit + ' OR ' + rn + ' IS NULL');
          }
        }
      });

      _.each(queries, function (q) {
        select.left_join.apply(select, _toConsumableArray(q));
      });

      return select;
    default:
      return null;
  }
}

var Generator = (function () {
  function Generator(engine) {
    _classCallCheck(this, Generator);

    this._engine = engine ? engine.toLowerCase() : null;
    this._squel = initSquelForSpecificEngine(this._engine);
  }

  _createClass(Generator, [{
    key: 'select',
    value: function select(criteria) {
      var options = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];

      var include = criteria.getInclude();
      var modelName = criteria.getModelClass().getModelName();
      var select;
      if (include && include.length && this._engine) {
        select = processEngineSpecificJoinQuery(this._squel, this._engine, criteria);
        if (!select) {
          console.warn(this._engine + ' does not support include');
        }
        return select.toString();
      }

      select = this._squel.select().from(modelName);

      // always explicitly specify fields
      var fields = criteria.getFields();
      if (!fields || !fields.length) {
        var properties = criteria.getModelClass().getAllProperties();
        criteria.fields.apply(criteria, _toConsumableArray(properties));
      }

      select = generateCriteria('select', select, criteria);

      return select.toString();
    }
  }, {
    key: 'insert',
    value: function insert(criteria) {
      var options = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];

      var attributes = criteria.getAttributes();
      var modelName = criteria.getModelClass().getModelName();
      if (!_.isArray(attributes) && !_.isObject(attributes) || !_.size(attributes)) {
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
  }, {
    key: 'update',
    value: function update(criteria) {
      var options = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];

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
  }, {
    key: 'delete',
    value: function _delete(criteria) {
      var options = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];

      var modelName = criteria.getModelClass().getModelName();
      var del = this._squel.delete().from(modelName);
      del = generateCriteria('delete', del, criteria);

      del = processEngineSpecificDeleteQuery(del, this._engine);

      return del.toString();
    }
  }, {
    key: 'count',
    value: function count(criteria) {
      var options = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];

      var modelName = criteria.getModelClass().getModelName();
      var primaryKey = criteria.getModelClass().getPrimaryKey();
      var where = generateWhereStatement(criteria.getWhere());

      var select = this._squel.select().from(modelName).field('COUNT(' + primaryKey + ')').where(where.toString());

      return select.toString();
    }
  }]);

  return Generator;
})();

Generator.OPERATORS = OPERATORS;
Generator.PSQL = PSQL;

module.exports = Generator;