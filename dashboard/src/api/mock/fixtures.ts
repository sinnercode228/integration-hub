// Static demo configuration: mirrors config/relay.yaml of the backend (fictional data only).
import type { ConnectorsInfo } from '../types'

export const RETRY_SCHEDULE = [2, 4, 8, 16, 32, 64, 128]
export const DEFAULT_MAX_ATTEMPTS = RETRY_SCHEDULE.length + 1
export const MAX_ATTEMPTS: Record<string, number> = { 'amocrm-leads': 12 }

export const CONNECTORS: ConnectorsInfo = {
  sources: [
    { id: 'tilda-site', connector: 'tilda', enabled: true, signed: true, description: 'Contact forms and cart orders from the storefront (Tilda)' },
    { id: 'amocrm', connector: 'amocrm', enabled: true, signed: true, description: 'amoCRM account webhooks (URL contains ?token=...)' },
    { id: 'bitrix', connector: 'bitrix24', enabled: true, signed: true, description: 'Bitrix24 event handlers (application_token)' },
    { id: 'partner-api', connector: 'generic', enabled: true, signed: true, description: 'Any system that can sign JSON with HMAC-SHA256' },
  ],
  destinations: [
    { id: 'sales-telegram', connector: 'telegram', enabled: true, description: 'Sales team chat' },
    { id: 'leads-sheet', connector: 'google_sheets', enabled: true, description: 'Lead log for the marketing team' },
    { id: 'amocrm-leads', connector: 'amocrm_lead', enabled: true, description: 'Create a lead + contact in amoCRM' },
    { id: 'manager-email', connector: 'smtp', enabled: true, description: 'Order notification for the shop manager' },
    { id: 'warehouse-webhook', connector: 'webhook', enabled: true, description: 'Fulfilment system (signed JSON)' },
  ],
  routes: [
    {
      name: 'site-leads',
      description: 'Every form on the site -> chat, sheet and CRM',
      enabled: true,
      match: { source: ['tilda-site'], type: ['form.submitted'], where: [] },
      deliver: [
        { to: 'sales-telegram', template: { text: '<b>Новая заявка с сайта</b>\nИмя: {{ contact.name }}\nТелефон: {{ contact.phone | phone }}' } },
        { to: 'leads-sheet', template: { values: ["{{ received_at | date('%d.%m.%Y %H:%M') }}", '{{ contact.name }}', '{{ contact.phone | phone }}', '{{ id }}'] } },
        { to: 'amocrm-leads', template: { name: "Сайт: {{ fields.form_name | default('заявка') }}", contact: { name: '{{ contact.name }}', phone: '{{ contact.phone }}' } } },
      ],
    },
    {
      name: 'site-orders',
      description: 'Paid cart orders -> manager e-mail, warehouse, chat',
      enabled: true,
      match: { source: ['tilda-site'], type: ['order.created'], where: [] },
      deliver: [
        { to: 'manager-email', template: { subject: 'Заказ {{ fields.order.id }} на {{ fields.order.amount }} ₽' } },
        { to: 'warehouse-webhook', template: { order_id: '{{ fields.order.id }}', amount: '{{ fields.order.amount | float }}' } },
        { to: 'sales-telegram', template: { text: '<b>Заказ {{ fields.order.id }}</b> на {{ fields.order.amount }} ₽' } },
      ],
    },
    {
      name: 'crm-won-deals',
      description: 'amoCRM lead moved to "won" (status 142) -> sheet + warehouse',
      enabled: true,
      match: { source: ['amocrm'], type: ['lead.status_changed'], where: [{ path: 'fields.status_id', op: 'eq', value: 142 }] },
      deliver: [
        { to: 'leads-sheet', template: { values: ['{{ received_at | date }}', 'amoCRM', '{{ fields.name }}', '{{ fields.price | int }}', 'won'] } },
        { to: 'warehouse-webhook', template: null },
      ],
    },
    {
      name: 'bitrix-new-leads',
      description: 'New Bitrix24 leads -> sales chat',
      enabled: true,
      match: { source: ['bitrix'], type: ['lead.created', 'deal.created'], where: [] },
      deliver: [{ to: 'sales-telegram', template: { text: '<b>Bitrix24: {{ type }}</b> #{{ fields.entity_id }}' } }],
    },
    {
      name: 'partner-events',
      description: 'Everything from the partner API -> warehouse (as is)',
      enabled: true,
      match: { source: ['partner-api'], type: ['*'], where: [] },
      deliver: [{ to: 'warehouse-webhook', template: null }],
    },
  ],
  stock: { connector: 'moysklad' },
  available: {
    inbound: [
      { kind: 'amocrm', label: 'amoCRM' },
      { kind: 'bitrix24', label: 'Bitrix24' },
      { kind: 'generic', label: 'Generic JSON (HMAC)' },
      { kind: 'tilda', label: 'Tilda' },
    ],
    outbound: [
      { kind: 'amocrm_lead', label: 'amoCRM: create lead' },
      { kind: 'google_sheets', label: 'Google Sheets' },
      { kind: 'smtp', label: 'E-mail (SMTP)' },
      { kind: 'telegram', label: 'Telegram' },
      { kind: 'webhook', label: 'Webhook (HTTP)' },
    ],
  },
  retry_schedule_seconds: RETRY_SCHEDULE,
}

export const CONNECTOR_OF: Record<string, string> = Object.fromEntries(
  CONNECTORS.destinations.map((d) => [d.id, d.connector]),
)

// Fictional people and products for synthetic events.
export const PEOPLE = [
  ['Анна Смирнова', 'anna.smirnova@example.com'],
  ['Игорь Петров', 'igor.p@example.com'],
  ['Мария Кузнецова', 'maria.k@example.org'],
  ['Дмитрий Соколов', 'd.sokolov@example.com'],
  ['Елена Волкова', 'elena.v@example.net'],
  ['Павел Морозов', 'pavel.m@example.com'],
  ['Ольга Новикова', 'olga.n@example.org'],
  ['Сергей Лебедев', 's.lebedev@example.com'],
  ['Наталья Егорова', 'n.egorova@example.net'],
  ['Алексей Орлов', 'a.orlov@example.com'],
] as const

export const FORMS = ['Обратный звонок', 'Заявка на консультацию', 'Оптовый прайс', 'Подписка на новости']
export const PRODUCTS = [
  ['Кружка керамическая «Утро»', 'MUG-101', 890],
  ['Чай улун, 100 г', 'TEA-204', 640],
  ['Набор пиал, 4 шт.', 'SET-310', 2350],
  ['Чайник глиняный 0,6 л', 'POT-412', 3900],
  ['Подставка из дуба', 'OAK-520', 1150],
] as const
export const UTM = ['yandex', 'vk', 'telegram', 'direct', 'email']

export interface FailureSpec {
  status: number | null
  error: string
  retryable: boolean
  retryAfter?: number
}

// Realistic failure messages per connector (same wording as the backend produces).
export const FAILURES: Record<string, FailureSpec[]> = {
  telegram: [
    { status: 429, error: 'telegram: HTTP 429 - Too Many Requests: retry after 5', retryable: true, retryAfter: 5 },
    { status: 502, error: 'telegram: HTTP 502 - Bad Gateway', retryable: true },
  ],
  google_sheets: [
    { status: null, error: 'google_sheets: network error (ConnectTimeout: timed out)', retryable: true },
    { status: 503, error: 'google_sheets: HTTP 503 - The service is currently unavailable.', retryable: true },
  ],
  amocrm_lead: [
    { status: 401, error: 'amocrm_lead: HTTP 401 - access token is invalid or expired', retryable: false },
    { status: 504, error: 'amocrm_lead: HTTP 504 - Gateway Time-out', retryable: true },
  ],
  smtp: [
    { status: 451, error: 'smtp: 451 4.7.1 Greylisted, please try again later', retryable: true },
    { status: 550, error: 'smtp: 550 5.1.1 Mailbox unavailable', retryable: false },
  ],
  webhook: [
    { status: 503, error: 'webhook: HTTP 503 - {"error":"maintenance"}', retryable: true },
    { status: null, error: 'webhook: timeout (ReadTimeout)', retryable: true },
    { status: 422, error: 'webhook: HTTP 422 - {"error":"unknown sku"}', retryable: false },
  ],
}
