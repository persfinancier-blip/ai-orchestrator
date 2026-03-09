export const CLIENT_MODULE_SCHEMA = 'ao_clients';

function t(key, templateName, tableName, description, columns, extras = {}) {
  return Object.freeze({
    key,
    id: `builtin_${key}_table`,
    template_name: templateName,
    schema_name: CLIENT_MODULE_SCHEMA,
    table_name: tableName,
    data_level: 'silver',
    template_kind: 'data',
    table_class: 'client_data',
    description,
    columns: Object.freeze(columns.map((item) => Object.freeze({ ...item }))),
    partition_enabled: false,
    partition_column: '',
    partition_interval: 'day',
    ...extras
  });
}

export const CLIENT_MODULE_TEMPLATES = Object.freeze([
  t(
    'clients',
    'Клиенты',
    'clients',
    'Основной список клиентов для модуля «Клиенты». Используется как источник списка в левой колонке и как базовый реестр client_id.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор клиента.' },
      { field_name: 'client_code', field_type: 'text', description: 'Внутренний код клиента.' },
      { field_name: 'client_display_name', field_type: 'label', description: 'Отображаемое имя клиента.' },
      { field_name: 'status', field_type: 'text', description: 'Статус клиента.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий по клиенту.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда клиент создан.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда клиент обновлён.' }
    ],
    { indexes: ['client_code', 'status'], unique_indexes: [['client_code']] }
  ),
  t(
    'client_main_data',
    'Основные данные клиента',
    'client_main_data',
    'Основная карточка клиента: код, названия, статус и основной комментарий.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор строки основных данных.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'client_code', field_type: 'text', description: 'Внутренний код клиента.' },
      { field_name: 'client_name', field_type: 'label', description: 'Юридическое или базовое имя клиента.' },
      { field_name: 'client_display_name', field_type: 'label', description: 'Отображаемое имя клиента.' },
      { field_name: 'status', field_type: 'text', description: 'Статус клиента.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий по клиенту.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ],
    { indexes: ['client_id'], unique_indexes: [['client_id']] }
  ),
  t(
    'client_legal_entities',
    'Юридические лица клиента',
    'client_legal_entities',
    'Список юридических лиц, связанных с клиентом.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор юрлица.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'legal_entity_name', field_type: 'label', description: 'Название юридического лица.' },
      { field_name: 'legal_entity_code', field_type: 'text', description: 'Внутренний код юрлица.' },
      { field_name: 'tax_id', field_type: 'text', description: 'ИНН / налоговый идентификатор.' },
      { field_name: 'country', field_type: 'text', description: 'Страна.' },
      { field_name: 'role', field_type: 'text', description: 'Роль юрлица для клиента.' },
      { field_name: 'is_active', field_type: 'boolean', description: 'Активно ли юрлицо.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ],
    { indexes: ['client_id', 'legal_entity_code', 'tax_id'] }
  ),
  t(
    'client_contracts',
    'Договоры клиента',
    'client_contracts',
    'Договоры, привязанные к клиенту и юридическим лицам.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор договора.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'legal_entity_id', field_type: 'bigint', description: 'Ссылка на юрлицо.' },
      { field_name: 'contract_number', field_type: 'text', description: 'Номер договора.' },
      { field_name: 'contract_name', field_type: 'label', description: 'Название договора.' },
      { field_name: 'contract_date', field_type: 'date', description: 'Дата договора.' },
      { field_name: 'date_start', field_type: 'date', description: 'Дата начала действия.' },
      { field_name: 'date_end', field_type: 'date', description: 'Дата окончания действия.' },
      { field_name: 'status', field_type: 'text', description: 'Статус договора.' },
      { field_name: 'file_url', field_type: 'url', description: 'Ссылка на файл договора.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ],
    { indexes: ['client_id', 'legal_entity_id', 'contract_number'] }
  ),
  t(
    'client_payment_terms',
    'Условия оплаты клиента',
    'client_payment_terms',
    'Настройка условий оплаты по клиенту и договору.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор условия оплаты.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'contract_id', field_type: 'bigint', description: 'Ссылка на договор.' },
      { field_name: 'fee_type', field_type: 'text', description: 'Тип оплаты: фикс, процент, смешанный.' },
      { field_name: 'fixed_amount', field_type: 'numeric', description: 'Фиксированная сумма.' },
      { field_name: 'percent_value', field_type: 'numeric', description: 'Процентное значение.' },
      { field_name: 'percent_base', field_type: 'text', description: 'База для расчёта процента.' },
      { field_name: 'min_amount', field_type: 'numeric', description: 'Минимальная сумма.' },
      { field_name: 'max_amount', field_type: 'numeric', description: 'Максимальная сумма.' },
      { field_name: 'calc_period', field_type: 'text', description: 'Период расчёта.' },
      { field_name: 'is_active', field_type: 'boolean', description: 'Активно ли условие.' },
      { field_name: 'date_start', field_type: 'date', description: 'Дата начала действия.' },
      { field_name: 'date_end', field_type: 'date', description: 'Дата окончания действия.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ],
    { indexes: ['client_id', 'contract_id'] }
  ),
  t(
    'client_payment_schedule',
    'График платежей клиента',
    'client_payment_schedule',
    'График платежей, основанный на условиях оплаты клиента.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор строки графика.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'payment_term_id', field_type: 'bigint', description: 'Ссылка на условие оплаты.' },
      { field_name: 'payment_type', field_type: 'text', description: 'Тип платежа.' },
      { field_name: 'frequency', field_type: 'text', description: 'Частота платежа.' },
      { field_name: 'due_day', field_type: 'int', description: 'День оплаты.' },
      { field_name: 'amount', field_type: 'numeric', description: 'Сумма платежа.' },
      { field_name: 'calc_rule', field_type: 'text', description: 'Правило расчёта.' },
      { field_name: 'status', field_type: 'text', description: 'Статус графика.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ],
    { indexes: ['client_id', 'payment_term_id'] }
  ),
  t(
    'client_goals',
    'Цели клиента',
    'client_goals',
    'Цели клиента по росту, эффективности и развитию.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор цели.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'goal_name', field_type: 'label', description: 'Название цели.' },
      { field_name: 'goal_type', field_type: 'text', description: 'Тип цели.' },
      { field_name: 'priority', field_type: 'int', description: 'Приоритет.' },
      { field_name: 'date_start', field_type: 'date', description: 'Дата начала.' },
      { field_name: 'date_end', field_type: 'date', description: 'Дата окончания.' },
      { field_name: 'is_active', field_type: 'boolean', description: 'Активна ли цель.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ],
    { indexes: ['client_id', 'goal_type'] }
  ),
  t(
    'client_kpis',
    'KPI клиента',
    'client_kpis',
    'Показатели эффективности клиента и целей.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор KPI.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'goal_id', field_type: 'bigint', description: 'Ссылка на цель.' },
      { field_name: 'kpi_name', field_type: 'label', description: 'Название KPI.' },
      { field_name: 'kpi_type', field_type: 'text', description: 'Тип KPI.' },
      { field_name: 'target_value', field_type: 'numeric', description: 'Целевое значение.' },
      { field_name: 'min_value', field_type: 'numeric', description: 'Минимальное значение.' },
      { field_name: 'max_value', field_type: 'numeric', description: 'Максимальное значение.' },
      { field_name: 'unit', field_type: 'text', description: 'Единица измерения.' },
      { field_name: 'evaluation_period', field_type: 'text', description: 'Период оценки.' },
      { field_name: 'priority', field_type: 'int', description: 'Приоритет.' },
      { field_name: 'is_active', field_type: 'boolean', description: 'Активен ли KPI.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ],
    { indexes: ['client_id', 'goal_id', 'kpi_type'] }
  ),
  t(
    'client_accesses',
    'Платформы и доступы клиента',
    'client_accesses',
    'Доступы клиента к рекламным платформам, кабинетам и внешним сервисам.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор доступа.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'platform_code', field_type: 'text', description: 'Код платформы.' },
      { field_name: 'platform_name', field_type: 'label', description: 'Название платформы.' },
      { field_name: 'system_name', field_type: 'text', description: 'Название в системе.' },
      { field_name: 'external_id', field_type: 'text', description: 'Внешний идентификатор.' },
      { field_name: 'cabinet_name', field_type: 'label', description: 'Название кабинета.' },
      { field_name: 'cabinet_id', field_type: 'text', description: 'Идентификатор кабинета.' },
      { field_name: 'auth_type', field_type: 'text', description: 'Тип авторизации.' },
      { field_name: 'token_value', field_type: 'text_payload', description: 'Токен доступа.' },
      { field_name: 'login_value', field_type: 'text', description: 'Логин.' },
      { field_name: 'password_value', field_type: 'text_payload', description: 'Пароль.' },
      { field_name: 'api_key', field_type: 'text_payload', description: 'API-ключ.' },
      { field_name: 'access_scope', field_type: 'text', description: 'Область доступа.' },
      { field_name: 'access_mode', field_type: 'text', description: 'Режим доступа.' },
      { field_name: 'is_active', field_type: 'boolean', description: 'Активен ли доступ.' },
      { field_name: 'check_status', field_type: 'text', description: 'Статус проверки доступа.' },
      { field_name: 'last_checked_at', field_type: 'timestamptz', description: 'Когда доступ проверяли.' },
      { field_name: 'expires_at', field_type: 'timestamptz', description: 'Когда истекает токен или доступ.' },
      { field_name: 'last_error_text', field_type: 'text', description: 'Последняя ошибка.' },
      { field_name: 'data_table_ref', field_type: 'table_ref', description: 'Ссылка на таблицу данных.' },
      { field_name: 'sync_table_ref', field_type: 'table_ref', description: 'Ссылка на таблицу синхронизации.' },
      { field_name: 'log_table_ref', field_type: 'table_ref', description: 'Ссылка на таблицу логов.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ],
    { indexes: ['client_id', 'platform_code', 'is_active'] }
  ),
  t(
    'client_constraints',
    'Ограничения клиента',
    'client_constraints',
    'Ограничения, лимиты и важные рамки работы с клиентом.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор ограничения.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'constraint_type', field_type: 'text', description: 'Тип ограничения.' },
      { field_name: 'value', field_type: 'numeric', description: 'Значение ограничения.' },
      { field_name: 'unit', field_type: 'text', description: 'Единица ограничения.' },
      { field_name: 'date_start', field_type: 'date', description: 'Дата начала действия.' },
      { field_name: 'date_end', field_type: 'date', description: 'Дата окончания действия.' },
      { field_name: 'is_active', field_type: 'boolean', description: 'Активно ли ограничение.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ],
    { indexes: ['client_id', 'constraint_type'] }
  ),
  t(
    'client_summary_metrics',
    'Сводные метрики клиента',
    'client_summary_metrics',
    'Плановые и фактические показатели клиента для боковой сводки.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор строки метрик.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'budget_plan', field_type: 'numeric', description: 'План по бюджету.' },
      { field_name: 'budget_fact', field_type: 'numeric', description: 'Факт по бюджету.' },
      { field_name: 'revenue_plan', field_type: 'numeric', description: 'План по выручке.' },
      { field_name: 'revenue_fact', field_type: 'numeric', description: 'Факт по выручке.' },
      { field_name: 'orders_plan', field_type: 'numeric', description: 'План по заказам.' },
      { field_name: 'orders_fact', field_type: 'numeric', description: 'Факт по заказам.' },
      { field_name: 'drr_plan', field_type: 'numeric', description: 'Плановый ДРР.' },
      { field_name: 'drr_fact', field_type: 'numeric', description: 'Фактический ДРР.' },
      { field_name: 'roas_plan', field_type: 'numeric', description: 'Плановый ROAS.' },
      { field_name: 'roas_fact', field_type: 'numeric', description: 'Фактический ROAS.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда метрики обновлены.' }
    ],
    { indexes: ['client_id'], unique_indexes: [['client_id']] }
  ),
  t(
    'client_action_items',
    'Ближайшие действия клиента',
    'client_action_items',
    'Список ближайших действий, предупреждений и задач по клиенту.',
    [
      { field_name: 'id', field_type: 'bigint', description: 'Идентификатор действия.' },
      { field_name: 'client_id', field_type: 'bigint', description: 'Ссылка на клиента.' },
      { field_name: 'action_type', field_type: 'text', description: 'Тип действия.' },
      { field_name: 'title', field_type: 'label', description: 'Заголовок действия.' },
      { field_name: 'message', field_type: 'text', description: 'Подробности.' },
      { field_name: 'severity', field_type: 'text', description: 'Уровень важности.' },
      { field_name: 'due_at', field_type: 'timestamptz', description: 'Срок действия.' },
      { field_name: 'status', field_type: 'text', description: 'Статус действия.' },
      { field_name: 'is_active', field_type: 'boolean', description: 'Активно ли действие.' },
      { field_name: 'comment', field_type: 'text', description: 'Комментарий.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ],
    { indexes: ['client_id', 'status', 'is_active'] }
  )
]);

export const CLIENT_MODULE_SECTION_DEFINITIONS = Object.freeze({
  list: { key: 'clients', title: 'Список клиентов', mode: 'single' },
  main_data: { key: 'client_main_data', title: 'Основные данные', mode: 'single' },
  legal_entities: { key: 'client_legal_entities', title: 'Юр. лица', mode: 'multi' },
  contracts: { key: 'client_contracts', title: 'Договоры', mode: 'multi' },
  payment_terms: { key: 'client_payment_terms', title: 'Условия оплаты', mode: 'multi' },
  payment_schedule: { key: 'client_payment_schedule', title: 'График платежей', mode: 'multi' },
  goals: { key: 'client_goals', title: 'Цели', mode: 'multi' },
  kpis: { key: 'client_kpis', title: 'KPI', mode: 'multi' },
  accesses: { key: 'client_accesses', title: 'Платформы и доступы', mode: 'multi' },
  constraints: { key: 'client_constraints', title: 'Ограничения', mode: 'multi' },
  summary_metrics: { key: 'client_summary_metrics', title: 'План / факт', mode: 'single' },
  action_items: { key: 'client_action_items', title: 'Ближайшие действия', mode: 'multi' }
});

export function getClientModuleTemplateByKey(key) {
  return CLIENT_MODULE_TEMPLATES.find((item) => item.key === String(key || '').trim()) || null;
}

export function clientModuleSourceMeta() {
  return Object.fromEntries(
    CLIENT_MODULE_TEMPLATES.map((item) => [
      item.key,
      {
        key: item.key,
        template_name: item.template_name,
        schema_name: item.schema_name,
        table_name: item.table_name
      }
    ])
  );
}
