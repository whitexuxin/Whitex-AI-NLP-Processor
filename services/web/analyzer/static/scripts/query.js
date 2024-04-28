'use strict';

class ParameterDef {
  static TYPE_TEXT = 'text';
  static TYPE_TEXT_LIST = 'text_list';
  static TYPE_INT = 'int';
  static TYPE_FLOAT = 'float';
  static TYPE_COLUMN_NAME = 'column_name';
  static TYPE_COLUMN_NAME_LIST = 'column_name_list';
  static TYPE_DATE_RANGE = 'date_range';
  static TYPE_DATE_RANGE_LIST = 'date_range_list';

  static COLLECTION_TYPES = new Set([
    ParameterDef.TYPE_TEXT_LIST,
    ParameterDef.TYPE_DATE_RANGE_LIST,
    ParameterDef.TYPE_COLUMN_NAME_LIST,
  ]);

  static KEY_TYPE = 'type';
  static KEY_NAME = 'name';
  static KEY_LABEL = 'label';
  static KEY_EXAMPLE = 'example';

  static LIST_SEPARATOR = ',';
  static DATE_RANGE_SEPARATOR = ':';

  constructor(type, name, label, example) {
    this.type = type;
    this.name = name;
    this.label = label;
    this.example = example;
  }

  static fromDict(dict) {
    return new ParameterDef(
      dict[ParameterDef.KEY_TYPE],
      dict[ParameterDef.KEY_NAME],
      dict[ParameterDef.KEY_LABEL],
      dict[ParameterDef.KEY_EXAMPLE],
    );
  }

  static processValue(rawValue, type) {
    return new ValueProcessor(type).process(rawValue);


  }
}

class TransformDef {
  static KEY_TYPE = 'type';
  static KEY_DESC = 'description';
  static KEY_PARAMS = 'params';
  static KEY_OPS = 'ops';

  constructor(type, description, paramDefs, operations) {
    this._type = type;
    this._description = description;
    this._parameters = paramDefs;
    this._operations = operations;
  }

  get type() {
    return this._type;
  }

  get description() {
    return this._description;
  }

  get parameters() {
    return this._parameters;
  }

  get operations() {
    return this._operations;
  }

  static fromDict(dict) {
    return new TransformDef(
      dict[TransformDef.KEY_TYPE],
      dict[TransformDef.KEY_DESC],
      dict[TransformDef.KEY_PARAMS].map(p => ParameterDef.fromDict(p)),
      dict[TransformDef.KEY_OPS],
    );
  }
}


class Transform {
  constructor(transformDef, parameters, operation) {
    console.info(transformDef, parameters);
    this._def = transformDef;
    this.parameters = parameters;
    this.operation = operation;
  }

  get type() {
    return this._def.type;
  }

  get typeWithSpaces() {
    return this.type.replace(/([A-Z])/g, ' $1').toLocaleLowerCase().trim();
  }

  get description() {
    return this._def.description;
  }

  /*
  TODO: Add when tests exist
  get textDescription() {
    const tokens = [];
    for (const field of this.description) {
      if (field.startsWith('{') && field.endsWith('}')) {
        const key = field.substr(1, field.length - 2);
        tokens.push(this.parameters[key]);
      } else {
        tokens.push(field);
      }
    }

    return tokens.join('');
  }
   */

  get richTextDescription() {
    const VALUE_SPEC = '{}';
    const paragraph = document.createElement('p');

    for (const field of this.description) {
      if (field.startsWith(VALUE_SPEC[0]) && field.endsWith(VALUE_SPEC[1])) {

        const key = field.substr(1, field.length - 2);
        paragraph.appendChild(createSpan({
          cls: 'transformDescriptionFieldValue',
          text: this.parameters[key] || "''",
        }));

      } else {
        paragraph.appendChild(createSpan({
          cls: 'transformDescriptionFieldOperator',
          text: field,
        }));
      }
    }

    return paragraph;
  }

  toDict() {
    return {
      name: this.type,
      args: this.parameters,
    };
  }

  serialize() {
    const parameterDefs = this._def.parameters;
    const parameters = this.parameters;

    const values = [this.type, this.operation];
    for (const parameterDef of parameterDefs) {
      values.push(new ParameterProcessor(parameters, parameterDef).process());
      /*

      const parameterType = parameterDef.type;
      const parameterName = parameterDef.name;

      const value = parameters[parameterName];
      values.push(ParameterDef.processValue(value, parameterType));
       */

    }

    return values;
  }

  static deserialize(values) {
    /*
    Transforms are serialized as follows:
      [transformType, param_0, param_1, ..., param_n]
     */
    const transformType = values.shift();
    const operation = values.shift();

    const transformDef = app.transformManager.transformDefByType(transformType);
    const parameterDefs = transformDef.parameters;
    const parameters = {};

    for (const parameterDef of parameterDefs) {
      const parameterType = parameterDef.type;
      const parameterName = parameterDef.name;

      const value = values.shift();
      parameters[parameterName] = ParameterDef.processValue(value, parameterType);
      //parameters[parameterName] = values.shift();
    }

    return new Transform(
      transformDef,
      parameters,
      operation,
    )
  }
}


class Dataset {
  constructor(id, filename, name) {
    this.id = id;
    this.filename = filename;
    this.name = name;
  }

  static fromDict(dict) {
    return new Dataset(dict[0], dict[1], dict[2]);
  }
}

class Label {
  constructor(name, width, fontSize) {
    this.name = name;
    this.width = parseInt(width);
    this.fontSize = parseFloat(fontSize);
  }

  get hash() {
    return this.name.replace(/ /g, '_');
  }

  static deserialize(dict) {
    const KEY_NAME = 'n';
    const KEY_WIDTH = 'w';
    const KEY_FONT_SIZE = 's';
    return new 