-- Вот как можно вставить, обновить и удалить строку в таблице модулей:
--
-- INSERT INTO modules VALUES ('json', 14, 'format functions for JSON');
-- UPDATE modules SET size = 15 WHERE name = 'json';
-- DELETE FROM modules WHERE name = 'json';
--
-- Соответствующие не-SQL запросы Tarantool’а будут такими:
--
-- box.space.MODULES:insert{'json', 14, 'format functions for JSON'}
-- box.space.MODULES:update('json', {{'=', 2, 15}})
-- box.space.MODULES:delete{'json'}

-- Подключится к контейнеру к консоли для команд
docker exec -i -t tarantool console

-- Создание подключения
box.cfg({listen="127.0.0.1:3301"})

-- Создание пользователя для подключения
box.schema.user.create('user', {password='password', if_not_exists=true})

-- Все права для пользователя
box.schema.user.grant('user', 'super', nil, nil, {if_not_exists=true})
-- box.schema.user.grant('guest', 'read,write,execute', 'universe')

-- Создание схемы (таблицы) clients
box.schema.space.create('clients', {if_not_exists = true})

box.space.clients:format({
  {name = 'msisdn', type = 'unsigned'},
  {name = 'gender', type = 'string'},
  {name = 'age', type = 'unsigned'},
  {name = 'income', type = 'double'},
  {name = 'nextuse', type = 'unsigned'}
})

-- Создание индексов clients
box.space.clients:create_index('primary', {
  type = 'hash',
  unique = true,
  parts = {'msisdn'}
})

box.space.clients:create_index('nextuse', {
  type = 'tree',
  unique = false,
  parts = {'nextuse'}
})

-- Вставка данных clients
box.space.clients:insert{79000000001, 'M', 20, 10000.11, 0}
box.space.clients:insert{79000000002, 'F', 51, 22123.05, 0}
box.space.clients:insert{79000000003, 'F', 19, 5000.05, 1}

-- Получение данных clients
box.space.clients:select{}
box.space.clients:select(nil, {limit=10})
box.space.clients.index.nextuse:select({10}, {iterator='LT',limit=100})

-- Обновление данных clients
box.space.clients:update(79000000001, {{'=', 5, 10}})
box.space.clients:update(79000000001, {{'=', 'nextuse', 11}})

-- Удаление всех данных clients
box.space.clients:truncate()


-- Создание схемы (таблицы) segments
box.schema.space.create('segments', {if_not_exists = true})

box.space.segments:format({
  {name = 'id', type = 'uuid'},
  {name = 'msisdn', type = 'unsigned'}
})

-- Создание индексов segments
box.space.segments:create_index('primary', {unique = true, parts = {
  {field = 'id', type = 'uuid'},
  {field = 'msisdn', type = 'unsigned'}
}})

-- Вставка данных segments
box.space.segments:insert{uuid.fromstr('64d22e4d-ac92-4a23-899a-e59f34af5479'), 79000000001}
box.space.segments:insert{uuid.fromstr('64d22e4d-ac92-4a23-899a-e59f34af5479'), 79000000002}

-- Получение данных segments
box.space.segments:select{}
box.space.segments:select(nil, {limit=1})
box.space.segments.index.primary:select(
  {uuid.fromstr('64d22e4d-ac92-4a23-899a-e59f34af5478')}, {iterator='EQ'}
)



