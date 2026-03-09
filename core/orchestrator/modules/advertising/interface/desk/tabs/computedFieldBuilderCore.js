function deepClone(value) {
  return value && typeof value === 'object' ? JSON.parse(JSON.stringify(value)) : value;
}

export const COMPUTED_FUNCTION_CATEGORIES = [
  { id: 'math', label: 'Простая математика' },
  { id: 'condition', label: 'Условие' },
  { id: 'logic', label: 'Логика' },
  { id: 'text', label: 'Текст' },
  { id: 'date_time', label: 'Дата и время' }
];

const FUNCTION_DEFINITIONS = [
  {
    key: 'add',
    category: 'math',
    label: 'Сложение',
    description: 'Складывает числа или значения полей.',
    example: '{price} + {discount}',
    argMode: 'variadic',
    minArgs: 2,
    allowedKinds: ['number', 'field', 'expression']
  },
  {
    key: 'subtract',
    category: 'math',
    label: 'Вычитание',
    description: 'Вычитает второй и следующие операнды из первого.',
    example: '{price} - {discount}',
    argMode: 'variadic',
    minArgs: 2,
    allowedKinds: ['number', 'field', 'expression']
  },
  {
    key: 'multiply',
    category: 'math',
    label: 'Умножение',
    description: 'Перемножает числа или значения полей.',
    example: '{price} * 0.9',
    argMode: 'variadic',
    minArgs: 2,
    allowedKinds: ['number', 'field', 'expression']
  },
  {
    key: 'divide',
    category: 'math',
    label: 'Деление',
    description: 'Делит первый операнд на второй и следующие.',
    example: '{revenue} / {orders}',
    argMode: 'variadic',
    minArgs: 2,
    allowedKinds: ['number', 'field', 'expression']
  },
  {
    key: 'min',
    category: 'math',
    label: 'Минимум',
    description: 'Возвращает минимальное значение из списка.',
    example: 'минимум({price}, {base_price})',
    argMode: 'variadic',
    minArgs: 2,
    allowedKinds: ['number', 'field', 'expression']
  },
  {
    key: 'max',
    category: 'math',
    label: 'Максимум',
    description: 'Возвращает максимальное значение из списка.',
    example: 'максимум({price}, {base_price})',
    argMode: 'variadic',
    minArgs: 2,
    allowedKinds: ['number', 'field', 'expression']
  },
  {
    key: 'round',
    category: 'math',
    label: 'Округление',
    description: 'Округляет число до указанного количества знаков.',
    example: 'округлить({price}, 2)',
    argMode: 'fixed',
    args: [
      { key: 'value', label: 'Что округлить', allowedKinds: ['number', 'field', 'expression'] },
      { key: 'digits', label: 'Знаков после запятой', allowedKinds: ['number', 'field', 'expression'] }
    ]
  },
  {
    key: 'if',
    category: 'condition',
    label: 'Если',
    description: 'Возвращает одно значение, если условие истинно, и другое — если ложно.',
    example: 'если({price} > 0, {price} * 0.9, 0)',
    argMode: 'if'
  },
  {
    key: 'and',
    category: 'logic',
    label: 'И',
    description: 'Все условия должны быть истинны.',
    example: 'и({is_active}, не(пусто({sku})))',
    argMode: 'variadic',
    minArgs: 2,
    allowedKinds: ['boolean', 'field', 'expression']
  },
  {
    key: 'or',
    category: 'logic',
    label: 'Или',
    description: 'Хотя бы одно условие должно быть истинно.',
    example: 'или({is_new}, {is_sale})',
    argMode: 'variadic',
    minArgs: 2,
    allowedKinds: ['boolean', 'field', 'expression']
  },
  {
    key: 'not',
    category: 'logic',
    label: 'Не',
    description: 'Инвертирует логическое значение.',
    example: 'не({is_deleted})',
    argMode: 'fixed',
    args: [{ key: 'value', label: 'Значение', allowedKinds: ['boolean', 'field', 'expression'] }]
  },
  {
    key: 'empty',
    category: 'logic',
    label: 'Пусто',
    description: 'Проверяет, что значение пустое.',
    example: 'пусто({vendor_code})',
    argMode: 'fixed',
    args: [{ key: 'value', label: 'Значение', allowedKinds: ['field', 'text', 'number', 'date', 'boolean', 'expression'] }]
  },
  {
    key: 'length',
    category: 'text',
    label: 'Длина',
    description: 'Возвращает длину строки.',
    example: 'длина({sku})',
    argMode: 'fixed',
    args: [{ key: 'value', label: 'Текст', allowedKinds: ['field', 'text', 'expression'] }]
  },
  {
    key: 'substring',
    category: 'text',
    label: 'Подстрока',
    description: 'Возвращает часть строки, начиная с указанной позиции.',
    example: 'подстрока({sku}, 0, 3)',
    argMode: 'fixed',
    args: [
      { key: 'value', label: 'Текст', allowedKinds: ['field', 'text', 'expression'] },
      { key: 'start', label: 'С какой позиции', allowedKinds: ['number', 'field', 'expression'] },
      { key: 'length', label: 'Сколько символов', allowedKinds: ['number', 'field', 'expression'] }
    ]
  },
  {
    key: 'upper',
    category: 'text',
    label: 'Верхний регистр',
    description: 'Переводит текст в верхний регистр.',
    example: 'верхний_регистр({title})',
    argMode: 'fixed',
    args: [{ key: 'value', label: 'Текст', allowedKinds: ['field', 'text', 'expression'] }]
  },
  {
    key: 'lower',
    category: 'text',
    label: 'Нижний регистр',
    description: 'Переводит текст в нижний регистр.',
    example: 'нижний_регистр({title})',
    argMode: 'fixed',
    args: [{ key: 'value', label: 'Текст', allowedKinds: ['field', 'text', 'expression'] }]
  },
  {
    key: 'replace',
    category: 'text',
    label: 'Заменить',
    description: 'Заменяет часть текста на другое значение.',
    example: 'заменить({title}, "WB", "Wildberries")',
    argMode: 'fixed',
    args: [
      { key: 'value', label: 'Текст', allowedKinds: ['field', 'text', 'expression'] },
      { key: 'from', label: 'Что заменить', allowedKinds: ['text', 'field', 'expression'] },
      { key: 'to', label: 'На что заменить', allowedKinds: ['text', 'field', 'expression'] }
    ]
  },
  {
    key: 'concat',
    category: 'text',
    label: 'Склеить строки',
    description: 'Объединяет несколько значений в один текст.',
    example: 'склеить({brand}, " ", {title})',
    argMode: 'variadic',
    minArgs: 2,
    allowedKinds: ['field', 'text', 'expression', 'number', 'date', 'boolean']
  },
  {
    key: 'today',
    category: 'date_time',
    label: 'Сегодня',
    description: 'Текущая дата.',
    example: 'сегодня()',
    argMode: 'none'
  },
  {
    key: 'now',
    category: 'date_time',
    label: 'Сейчас',
    description: 'Текущая дата и время.',
    example: 'сейчас()',
    argMode: 'none'
  },
  {
    key: 'year',
    category: 'date_time',
    label: 'Год',
    description: 'Возвращает год из даты.',
    example: 'год({created_at})',
    argMode: 'fixed',
    args: [{ key: 'value', label: 'Дата', allowedKinds: ['field', 'date', 'expression', 'text'] }]
  },
  {
    key: 'month',
    category: 'date_time',
    label: 'Месяц',
    description: 'Возвращает месяц из даты.',
    example: 'месяц({created_at})',
    argMode: 'fixed',
    args: [{ key: 'value', label: 'Дата', allowedKinds: ['field', 'date', 'expression', 'text'] }]
  },
  {
    key: 'day',
    category: 'date_time',
    label: 'День',
    description: 'Возвращает день месяца из даты.',
    example: 'день({created_at})',
    argMode: 'fixed',
    args: [{ key: 'value', label: 'Дата', allowedKinds: ['field', 'date', 'expression', 'text'] }]
  },
  {
    key: 'date',
    category: 'date_time',
    label: 'Дата',
    description: 'Собирает дату из года, месяца и дня.',
    example: 'дата(2026, 3, 9)',
    argMode: 'fixed',
    args: [
      { key: 'year', label: 'Год', allowedKinds: ['number', 'field', 'expression'] },
      { key: 'month', label: 'Месяц', allowedKinds: ['number', 'field', 'expression'] },
      { key: 'day', label: 'День', allowedKinds: ['number', 'field', 'expression'] }
    ]
  },
  {
    key: 'date_diff',
    category: 'date_time',
    label: 'Разница дат',
    description: 'Считает разницу между двумя датами.',
    example: 'дата_разница({date_to}, {date_from}, "day")',
    argMode: 'fixed',
    args: [
      { key: 'left', label: 'Первая дата', allowedKinds: ['field', 'date', 'expression', 'text'] },
      { key: 'right', label: 'Вторая дата', allowedKinds: ['field', 'date', 'expression', 'text'] },
      { key: 'unit', label: 'Единица измерения', allowedKinds: ['text', 'field', 'expression'] }
    ]
  }
];

export const COMPUTED_FUNCTIONS = FUNCTION_DEFINITIONS;
export const COMPUTED_FUNCTIONS_BY_KEY = Object.fromEntries(FUNCTION_DEFINITIONS.map((item) => [item.key, item]));
export const VALUE_SOURCE_OPTIONS = [
  { value: 'field', label: 'Поле' },
  { value: 'number', label: 'Число' },
  { value: 'text', label: 'Текст' },
  { value: 'boolean', label: 'Логическое значение' },
  { value: 'date', label: 'Дата' },
  { value: 'expression', label: 'Вложенное выражение' }
];
export const CONDITION_OPERATOR_OPTIONS = [
  { value: 'equals', label: 'Равно' },
  { value: 'not_equals', label: 'Не равно' },
  { value: 'gt', label: 'Больше' },
  { value: 'lt', label: 'Меньше' },
  { value: 'gte', label: 'Больше или равно' },
  { value: 'lte', label: 'Меньше или равно' },
  { value: 'contains', label: 'Содержит' },
  { value: 'not_contains', label: 'Не содержит' },
  { value: 'empty', label: 'Пусто' },
  { value: 'not_empty', label: 'Не пусто' }
];

const CATEGORY_DEFAULTS = {
  math: 'add',
  condition: 'if',
  logic: 'and',
  text: 'concat',
  date_time: 'today'
};

function defaultOperandForKinds(kinds = []) {
  if (kinds.includes('number')) return createExpressionOperand('number');
  if (kinds.includes('text')) return createExpressionOperand('text');
  if (kinds.includes('boolean')) return createExpressionOperand('boolean');
  if (kinds.includes('date')) return createExpressionOperand('date');
  return createExpressionOperand('field');
}

function createArgsForDefinition(definition) {
  if (!definition) return [];
  if (definition.argMode === 'fixed') return definition.args.map((arg) => defaultOperandForKinds(arg.allowedKinds));
  if (definition.argMode === 'variadic') {
    return Array.from({ length: Math.max(2, definition.minArgs || 2) }, () => defaultOperandForKinds(definition.allowedKinds));
  }
  return [];
}

export function createExpressionOperand(sourceType = 'field') {
  return normalizeExpressionOperand({ sourceType });
}

export function createExpressionBuilder(functionKey = 'add') {
  const definition = COMPUTED_FUNCTIONS_BY_KEY[functionKey] || COMPUTED_FUNCTIONS_BY_KEY.add;
  return {
    category: definition.category,
    functionKey: definition.key,
    args: createArgsForDefinition(definition),
    condition: {
      left: createExpressionOperand('field'),
      operator: 'equals',
      right: createExpressionOperand('number')
    },
    whenTrue: createExpressionOperand('field'),
    whenFalse: createExpressionOperand('number')
  };
}

export function createComputedFieldRule() {
  const builder = createExpressionBuilder('add');
  return {
    name: '',
    mode: 'builder',
    type: '',
    expression: buildExpressionFromBuilder(builder),
    builder
  };
}

export function normalizeExpressionOperand(raw) {
  const src = raw && typeof raw === 'object' ? raw : {};
  const sourceType = ['field', 'number', 'text', 'boolean', 'date', 'expression'].includes(String(src.sourceType || '').trim())
    ? String(src.sourceType || '').trim()
    : 'field';
  return {
    sourceType,
    field: String(src.field || '').trim(),
    numberValue: String(src.numberValue ?? src.number_value ?? '').trim(),
    textValue: String(src.textValue ?? src.text_value ?? ''),
    booleanValue: String(src.booleanValue ?? src.boolean_value ?? 'false') === 'true' ? 'true' : 'false',
    dateValue: String(src.dateValue ?? src.date_value ?? '').trim(),
    builder: sourceType === 'expression' ? normalizeExpressionBuilder(src.builder) : null
  };
}

export function normalizeExpressionBuilder(raw) {
  const src = raw && typeof raw === 'object' ? raw : {};
  const category = COMPUTED_FUNCTION_CATEGORIES.some((item) => item.id === src.category) ? src.category : 'math';
  const functionKey = COMPUTED_FUNCTIONS_BY_KEY[src.functionKey] ? src.functionKey : CATEGORY_DEFAULTS[category] || 'add';
  const definition = COMPUTED_FUNCTIONS_BY_KEY[functionKey];
  const builder = {
    category: definition.category,
    functionKey,
    args: [],
    condition: {
      left: normalizeExpressionOperand(src.condition?.left),
      operator: CONDITION_OPERATOR_OPTIONS.some((item) => item.value === src.condition?.operator) ? src.condition.operator : 'equals',
      right: normalizeExpressionOperand(src.condition?.right)
    },
    whenTrue: normalizeExpressionOperand(src.whenTrue),
    whenFalse: normalizeExpressionOperand(src.whenFalse)
  };
  if (definition.argMode === 'fixed') {
    builder.args = definition.args.map((arg, index) => normalizeExpressionOperand(src.args?.[index] || defaultOperandForKinds(arg.allowedKinds)));
  } else if (definition.argMode === 'variadic') {
    const rawArgs = Array.isArray(src.args) && src.args.length ? src.args : createArgsForDefinition(definition);
    builder.args = rawArgs.map((arg) => normalizeExpressionOperand(arg));
    while (builder.args.length < Math.max(2, definition.minArgs || 2)) {
      builder.args.push(defaultOperandForKinds(definition.allowedKinds));
    }
  }
  return builder;
}

export function normalizeComputedFieldRule(raw) {
  const src = raw && typeof raw === 'object' ? raw : {};
  const hasBuilder = src.builder && typeof src.builder === 'object';
  const mode = String(src.mode || '').trim() === 'builder' || (!src.mode && hasBuilder) ? 'builder' : 'manual';
  const builder = normalizeExpressionBuilder(src.builder);
  const expression = String(src.expression || '').trim() || (mode === 'builder' ? buildExpressionFromBuilder(builder) : '');
  return {
    name: String(src.name || '').trim(),
    mode,
    type: String(src.type || '').trim(),
    expression,
    builder
  };
}

export function serializeComputedFieldRule(rule) {
  const normalized = normalizeComputedFieldRule(rule);
  return {
    name: normalized.name,
    mode: normalized.mode,
    type: normalized.type,
    expression: normalized.mode === 'builder' ? buildExpressionFromBuilder(normalized.builder) : normalized.expression,
    builder: deepClone(normalized.builder)
  };
}

export function duplicateComputedFieldRule(rule) {
  const normalized = normalizeComputedFieldRule(rule);
  return {
    ...deepClone(normalized),
    name: normalized.name ? `${normalized.name}_copy` : '',
    expression: normalized.mode === 'builder' ? buildExpressionFromBuilder(normalized.builder) : normalized.expression
  };
}

export function functionsByCategory(category) {
  return COMPUTED_FUNCTIONS.filter((item) => item.category === category);
}

function quoteText(value) {
  return JSON.stringify(String(value ?? ''));
}

function buildOperandExpression(operand) {
  const normalized = normalizeExpressionOperand(operand);
  if (normalized.sourceType === 'field') return `{${normalized.field || ''}}`;
  if (normalized.sourceType === 'number') return String(normalized.numberValue || '0');
  if (normalized.sourceType === 'text') return quoteText(normalized.textValue || '');
  if (normalized.sourceType === 'boolean') return normalized.booleanValue === 'true' ? 'true' : 'false';
  if (normalized.sourceType === 'date') return quoteText(normalized.dateValue || '');
  if (normalized.sourceType === 'expression') return buildExpressionFromBuilder(normalized.builder);
  return '';
}

function buildConditionExpression(condition) {
  const left = buildOperandExpression(condition?.left);
  const right = buildOperandExpression(condition?.right);
  const operator = String(condition?.operator || 'equals').trim();
  if (operator === 'equals') return `${left} == ${right}`;
  if (operator === 'not_equals') return `${left} != ${right}`;
  if (operator === 'gt') return `${left} > ${right}`;
  if (operator === 'lt') return `${left} < ${right}`;
  if (operator === 'gte') return `${left} >= ${right}`;
  if (operator === 'lte') return `${left} <= ${right}`;
  if (operator === 'contains') return `содержит(${left}, ${right})`;
  if (operator === 'not_contains') return `не_содержит(${left}, ${right})`;
  if (operator === 'empty') return `пусто(${left})`;
  if (operator === 'not_empty') return `не(пусто(${left}))`;
  return `${left} == ${right}`;
}

export function buildExpressionFromBuilder(rawBuilder) {
  const builder = normalizeExpressionBuilder(rawBuilder);
  const definition = COMPUTED_FUNCTIONS_BY_KEY[builder.functionKey] || COMPUTED_FUNCTIONS_BY_KEY.add;
  const args = builder.args.map((arg) => buildOperandExpression(arg));
  if (definition.key === 'add') return args.join(' + ');
  if (definition.key === 'subtract') return args.join(' - ');
  if (definition.key === 'multiply') return args.join(' * ');
  if (definition.key === 'divide') return args.join(' / ');
  if (definition.key === 'min') return `минимум(${args.join(', ')})`;
  if (definition.key === 'max') return `максимум(${args.join(', ')})`;
  if (definition.key === 'round') return `округлить(${args[0] || '0'}, ${args[1] || '0'})`;
  if (definition.key === 'if') {
    return `если(${buildConditionExpression(builder.condition)}, ${buildOperandExpression(builder.whenTrue)}, ${buildOperandExpression(builder.whenFalse)})`;
  }
  if (definition.key === 'and') return `и(${args.join(', ')})`;
  if (definition.key === 'or') return `или(${args.join(', ')})`;
  if (definition.key === 'not') return `не(${args[0] || 'false'})`;
  if (definition.key === 'empty') return `пусто(${args[0] || '""'})`;
  if (definition.key === 'length') return `длина(${args[0] || '""'})`;
  if (definition.key === 'substring') return `подстрока(${args[0] || '""'}, ${args[1] || '0'}${args[2] ? `, ${args[2]}` : ''})`;
  if (definition.key === 'upper') return `верхний_регистр(${args[0] || '""'})`;
  if (definition.key === 'lower') return `нижний_регистр(${args[0] || '""'})`;
  if (definition.key === 'replace') return `заменить(${args[0] || '""'}, ${args[1] || '""'}, ${args[2] || '""'})`;
  if (definition.key === 'concat') return `склеить(${args.join(', ')})`;
  if (definition.key === 'today') return 'сегодня()';
  if (definition.key === 'now') return 'сейчас()';
  if (definition.key === 'year') return `год(${args[0] || '""'})`;
  if (definition.key === 'month') return `месяц(${args[0] || '""'})`;
  if (definition.key === 'day') return `день(${args[0] || '""'})`;
  if (definition.key === 'date') return `дата(${args[0] || '0'}, ${args[1] || '0'}, ${args[2] || '0'})`;
  if (definition.key === 'date_diff') return `дата_разница(${args[0] || '""'}, ${args[1] || '""'}, ${args[2] || '"day"'})`;
  return args[0] || '';
}

export function buildComputedFieldPreview(rule) {
  const normalized = normalizeComputedFieldRule(rule);
  return normalized.mode === 'builder' ? buildExpressionFromBuilder(normalized.builder) : normalized.expression;
}

export function buildComputedFieldTypeWarnings(builder, fieldTypes = {}) {
  const warnings = [];
  const normalized = normalizeExpressionBuilder(builder);
  const definition = COMPUTED_FUNCTIONS_BY_KEY[normalized.functionKey];
  if (!definition) return warnings;
  const argsToCheck =
    definition.argMode === 'if'
      ? [
          { label: 'Левая часть условия', operand: normalized.condition.left, expectedKinds: ['field', 'number', 'text', 'date', 'expression', 'boolean'] },
          { label: 'Правая часть условия', operand: normalized.condition.right, expectedKinds: ['field', 'number', 'text', 'date', 'expression', 'boolean'] },
          { label: 'Значение, если условие истинно', operand: normalized.whenTrue, expectedKinds: ['field', 'number', 'text', 'date', 'expression', 'boolean'] },
          { label: 'Значение, если условие ложно', operand: normalized.whenFalse, expectedKinds: ['field', 'number', 'text', 'date', 'expression', 'boolean'] }
        ]
      : normalized.args.map((operand, index) => ({
          label: definition.argMode === 'fixed' ? definition.args[index]?.label || `Аргумент ${index + 1}` : `Операнд ${index + 1}`,
          operand,
          expectedKinds:
            definition.argMode === 'fixed'
              ? definition.args[index]?.allowedKinds || []
              : definition.allowedKinds || []
        }));
  for (const item of argsToCheck) {
    const operand = normalizeExpressionOperand(item.operand);
    if (operand.sourceType !== 'field') continue;
    const fieldType = String(fieldTypes?.[operand.field] || '').trim().toLowerCase();
    if (!fieldType) continue;
    if (item.expectedKinds.includes('number') && /text|string|label|url/.test(fieldType)) {
      warnings.push(`${item.label}: поле "${operand.field}" похоже на текст, а функция ожидает число.`);
    }
    if (item.expectedKinds.includes('text') && /(int|numeric|number|boolean|date|timestamp)/.test(fieldType)) {
      warnings.push(`${item.label}: поле "${operand.field}" имеет нетекстовый тип, проверь выбранную функцию.`);
    }
    if (item.expectedKinds.includes('date') && !/(date|time|timestamp)/.test(fieldType)) {
      warnings.push(`${item.label}: поле "${operand.field}" не похоже на дату или время.`);
    }
    if (item.expectedKinds.includes('boolean') && !/(bool|boolean)/.test(fieldType)) {
      warnings.push(`${item.label}: поле "${operand.field}" не похоже на логическое значение.`);
    }
  }
  return warnings;
}
