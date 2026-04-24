import socket
import threading
import json
import sys
import traceback
import uuid
import time
import os

class DiscordServer:
    def __init__(self, host='0.0.0.0', port=None):
        self.host = host
        self.port = port or 12345
        self.server_socket = None
        self.data_file = "server_data.json"

        # (socket, file, username, current_channel, current_group, is_guest)
        self.clients = []
        self.channels = {
            'general': [],
            'random': [],
            'dev': []
        }
        self.history = {ch: [] for ch in self.channels}
        self.MAX_HISTORY = 100
        self.running = False
        self.lock = threading.Lock()

        # Профили пользователей: username -> {avatar, description, display_name}
        self.profiles = {}

        # Учетные данные: username -> {password, is_guest}
        self.credentials = {}

        # Личные чаты: frozenset({u1,u2}) -> [messages]
        self.dm_history = {}

        # Группы: group_id -> {name, owner, members, channels:{ch_name:[]}, voice_channels:[]}
        self.groups = {}
        self._group_counter = 0

        # Голосовые каналы: (group_id, vc_name) -> {members: {username: {ip, udp_port}}, streamers: set()}
        self.voice_rooms = {}

        # Система отложенного сохранения
        self._data_dirty = False
        self._save_timer = None
        self.SAVE_DELAY = 5.0  # Сохранять через 5 секунд после последнего изменения

        # Загружаем данные при старте
        self._load_data()

        # UDP сокет для маршрутизации аудио/видео
        self.udp_socket = None
        self.udp_port = 12346
        self._start_udp_server()

    def _start_udp_server(self):
        try:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.bind((self.host, self.udp_port))
            print(f"[OK] UDP сервер запущен на {self.host}:{self.udp_port}")
            threading.Thread(target=self._udp_relay_loop, daemon=True).start()
        except Exception as e:
            print(f"[ERROR] Ошибка запуска UDP сервера: {e}")

    def _udp_relay_loop(self):
        """Маршрутизация UDP пакетов между участниками голосовых каналов"""
        addr_cache = {}
        cache_time = {}
        CACHE_TTL = 5.0

        while self.running:
            try:
                data, addr = self.udp_socket.recvfrom(65535)
                if len(data) < 1:
                    continue

                current_time = time.time()
                addr_key = (addr[0], addr[1])

                # Проверяем кэш
                if addr_key in addr_cache and (current_time - cache_time.get(addr_key, 0)) < CACHE_TTL:
                    sender, room_key = addr_cache[addr_key]
                else:
                    # Находим отправителя по адресу
                    sender = None
                    room_key = None
                    with self.lock:
                        for key, room in self.voice_rooms.items():
                            for username, info in room['members'].items():
                                if info['ip'] == addr[0] and info['udp_port'] == addr[1]:
                                    sender = username
                                    room_key = key
                                    addr_cache[addr_key] = (sender, room_key)
                                    cache_time[addr_key] = current_time
                                    break
                            if sender:
                                break

                if not sender or not room_key:
                    continue

                # Пересылаем всем остальным участникам комнаты (без блокировки на отправку)
                targets = []
                with self.lock:
                    room = self.voice_rooms.get(room_key)
                    if room:
                        for username, info in room['members'].items():
                            if username != sender:
                                targets.append((info['ip'], info['udp_port']))

                for target in targets:
                    try:
                        self.udp_socket.sendto(data, target)
                    except Exception:
                        pass

            except Exception as e:
                if self.running:
                    print(f"UDP relay error: {e}")

    def _load_data(self):
        """Загрузка данных из файла"""
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.credentials = data.get('credentials', {})
                    self.profiles = data.get('profiles', {})
                    self.history = data.get('history', {ch: [] for ch in self.channels})
                    self.groups = data.get('groups', {})
                    self._group_counter = data.get('group_counter', 0)

                    # Восстанавливаем dm_history с frozenset ключами
                    dm_data = data.get('dm_history', {})
                    self.dm_history = {}
                    for key_str, messages in dm_data.items():
                        users = key_str.split('|')
                        if len(users) == 2:
                            self.dm_history[frozenset(users)] = messages

                    # Конвертируем members в set для групп
                    for gid, group in self.groups.items():
                        if 'members' in group and isinstance(group['members'], list):
                            group['members'] = set(group['members'])

                    print(f"[OK] Загружено: {len(self.credentials)} аккаунтов, {len(self.groups)} групп")
            except Exception as e:
                print(f"[WARNING] Ошибка загрузки данных: {e}")
        else:
            print("[INFO] Файл данных не найден, начинаем с чистого состояния")

    def _save_data(self):
        """Сохранение данных в файл"""
        try:
            # Конвертируем dm_history для JSON
            dm_data = {}
            for key, messages in self.dm_history.items():
                key_str = '|'.join(sorted(key))
                dm_data[key_str] = messages

            # Конвертируем members из set в list для групп
            groups_copy = {}
            for gid, group in self.groups.items():
                group_copy = dict(group)
                if 'members' in group_copy and isinstance(group_copy['members'], set):
                    group_copy['members'] = list(group_copy['members'])
                groups_copy[gid] = group_copy

            data = {
                'credentials': self.credentials,
                'profiles': self.profiles,
                'history': self.history,
                'groups': groups_copy,
                'group_counter': self._group_counter,
                'dm_history': dm_data
            }

            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[ERROR] Ошибка сохранения данных: {e}")

    def _mark_dirty(self):
        """Помечает данные как измененные и планирует отложенное сохранение"""
        self._data_dirty = True

        # Отменяем предыдущий таймер, если есть
        if self._save_timer:
            self._save_timer.cancel()

        # Создаем новый таймер
        self._save_timer = threading.Timer(self.SAVE_DELAY, self._delayed_save)
        self._save_timer.daemon = True
        self._save_timer.start()

    def _delayed_save(self):
        """Выполняет отложенное сохранение"""
        if self._data_dirty:
            self._save_data()
            self._data_dirty = False
            self._save_timer = None

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        self.running = True
        print(f"[OK] Сервер запущен на {self.host}:{self.port}")
        print("Доступные каналы:", list(self.channels.keys()))
        threading.Thread(target=self.accept_clients, daemon=True).start()

    def accept_clients(self):
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                print(f"[CONNECT] Новое подключение от {addr}")
                client_file = client_socket.makefile('r', encoding='utf-8')
                try:
                    first_line = client_file.readline()
                    if not first_line:
                        client_socket.close()
                        continue
                    msg = json.loads(first_line)
                    if msg.get('type') == 'register':
                        username = msg['username']
                        password = msg.get('password', '')
                        is_register = msg.get('is_register', False)
                        is_guest = msg.get('is_guest', False)

                        with self.lock:
                            # Проверяем, не занят ли никнейм онлайн
                            if any(u == username for _, _, u, _, _, _ in self.clients):
                                client_socket.send((json.dumps({'type': 'error', 'message': 'Пользователь уже онлайн'}) + '\n').encode())
                                client_socket.close()
                                continue

                            # Гостевой режим
                            if is_guest:
                                # Гости не сохраняются в credentials
                                if username not in self.profiles:
                                    self.profiles[username] = {
                                        'avatar': None,
                                        'description': '',
                                        'display_name': username
                                    }
                                self.clients.append((client_socket, client_file, username, 'general', None, True))
                            # Регистрация нового аккаунта
                            elif is_register:
                                if username in self.credentials:
                                    client_socket.send((json.dumps({'type': 'error', 'message': 'Логин уже занят'}) + '\n').encode())
                                    client_socket.close()
                                    continue
                                # Создаем новый аккаунт
                                self.credentials[username] = {
                                    'password': password,
                                    'is_guest': False
                                }
                                self.profiles[username] = {
                                    'avatar': None,
                                    'description': '',
                                    'display_name': username
                                }
                                self.clients.append((client_socket, client_file, username, 'general', None, False))
                                self._save_data()
                            # Вход в существующий аккаунт
                            else:
                                if username not in self.credentials:
                                    client_socket.send((json.dumps({'type': 'error', 'message': 'Аккаунт не найден'}) + '\n').encode())
                                    client_socket.close()
                                    continue
                                if self.credentials[username]['password'] != password:
                                    client_socket.send((json.dumps({'type': 'error', 'message': 'Неверный пароль'}) + '\n').encode())
                                    client_socket.close()
                                    continue
                                # Успешный вход
                                self.clients.append((client_socket, client_file, username, 'general', None, False))

                        self.send_to_client(client_socket, {
                            'type': 'registered',
                            'username': username,
                            'channels': list(self.channels.keys()),
                            'current_channel': 'general',
                            'history': self.history['general'],
                            'profile': self.profiles.get(username, {'avatar': None, 'description': '', 'display_name': username}),
                            'groups': self._get_user_groups(username)
                        })
                        self.broadcast_user_list()
                        threading.Thread(target=self.handle_client, args=(client_socket, client_file, username), daemon=True).start()
                except Exception as e:
                    print(f"Ошибка регистрации {addr}: {e}")
                    traceback.print_exc()
                    client_socket.close()
            except Exception as e:
                if self.running:
                    print(f"Ошибка accept: {e}")

    def _get_user_groups(self, username):
        result = []
        for gid, g in self.groups.items():
            if username in g['members']:
                result.append({
                    'id': gid,
                    'name': g['name'],
                    'owner': g['owner'],
                    'channels': list(g['channels'].keys()),
                    'voice_channels': g.get('voice_channels', []),
                    'members': list(g['members'])
                })
        return result

    def handle_client(self, client_socket, client_file, username):
        while self.running:
            try:
                line = client_file.readline()
                if not line:
                    break
                msg = json.loads(line)
                msg_type = msg.get('type')

                if msg_type == 'chat':
                    content = msg['content']
                    if content.startswith('/pm '):
                        parts = content.split(maxsplit=2)
                        if len(parts) >= 3:
                            self.send_private_message(username, parts[1], parts[2])
                        else:
                            self.send_to_client(client_socket, {'type': 'error', 'message': 'Использование: /pm ник сообщение'})
                    else:
                        current_channel = self.get_client_channel(username)
                        current_group = self.get_client_group(username)
                        # Используем msg_id от клиента, если есть, иначе генерируем новый
                        msg_id = msg.get('msg_id', str(uuid.uuid4()))
                        if current_group:
                            self.broadcast_to_group_channel(current_group, current_channel, {
                                'type': 'message',
                                'sender': username,
                                'content': content,
                                'channel': current_channel,
                                'group_id': current_group,
                                'msg_id': msg_id
                            }, exclude_socket=client_socket)
                            with self.lock:
                                if current_group in self.groups and current_channel in self.groups[current_group]['channels']:
                                    self.groups[current_group]['channels'][current_channel].append({
                                        'sender': username, 'content': content, 'is_image': False, 'msg_id': msg_id
                                    })
                                    self._mark_dirty()
                        elif current_channel:
                            self.broadcast_to_channel(current_channel, {
                                'type': 'message',
                                'sender': username,
                                'content': content,
                                'channel': current_channel,
                                'msg_id': msg_id
                            }, exclude_socket=client_socket)
                            with self.lock:
                                self.history[current_channel].append({
                                    'sender': username, 'content': content, 'is_image': False, 'msg_id': msg_id
                                })
                                if len(self.history[current_channel]) > self.MAX_HISTORY:
                                    self.history[current_channel].pop(0)
                                self._mark_dirty()

                elif msg_type == 'dm':
                    to_user = msg['to']
                    content = msg['content']
                    msg_id = msg.get('msg_id', str(uuid.uuid4()))
                    self._handle_dm(username, to_user, content, is_image=False, msg_id=msg_id)

                elif msg_type == 'dm_image':
                    to_user = msg['to']
                    image_data = msg['image_data']
                    msg_id = msg.get('msg_id', str(uuid.uuid4()))
                    self._handle_dm(username, to_user, image_data, is_image=True, msg_id=msg_id)

                elif msg_type == 'dm_file':
                    to_user = msg['to']
                    file_data = msg['file_data']
                    file_name = msg['file_name']
                    file_type = msg.get('file_type', 'unknown')
                    msg_id = msg.get('msg_id', str(uuid.uuid4()))
                    self._handle_dm(username, to_user, file_data, is_image=False, is_file=True, file_name=file_name, file_type=file_type, msg_id=msg_id)

                elif msg_type == 'get_dm_history':
                    with_user = msg['with']
                    key = frozenset({username, with_user})
                    with self.lock:
                        hist = self.dm_history.get(key, [])
                    self.send_to_client(client_socket, {
                        'type': 'dm_history',
                        'with': with_user,
                        'history': hist
                    })

                elif msg_type == 'switch_channel':
                    new_channel = msg['channel']
                    group_id = msg.get('group_id', None)
                    if group_id:
                        with self.lock:
                            ok = group_id in self.groups and new_channel in self.groups[group_id]['channels']
                        if ok:
                            self.set_client_channel(username, new_channel, group_id)
                            with self.lock:
                                history = list(self.groups[group_id]['channels'][new_channel])
                            self.send_to_client(client_socket, {
                                'type': 'channel_switched',
                                'channel': new_channel,
                                'history': history,
                                'group_id': group_id
                            })
                        else:
                            self.send_to_client(client_socket, {'type': 'error', 'message': 'Канал не существует'})
                    else:
                        if new_channel in self.channels:
                            self.set_client_channel(username, new_channel, None)
                            with self.lock:
                                history = self.history[new_channel].copy()
                            self.send_to_client(client_socket, {
                                'type': 'channel_switched',
                                'channel': new_channel,
                                'history': history,
                                'group_id': None
                            })
                        else:
                            self.send_to_client(client_socket, {'type': 'error', 'message': 'Канал не существует'})

                elif msg_type == 'image':
                    image_data = msg['image_data']
                    current_channel = self.get_client_channel(username)
                    current_group = self.get_client_group(username)
                    # Используем msg_id от клиента, если есть, иначе генерируем новый
                    msg_id = msg.get('msg_id', str(uuid.uuid4()))
                    if current_group:
                        self.broadcast_to_group_channel(current_group, current_channel, {
                            'type': 'image_message',
                            'sender': username,
                            'image_data': image_data,
                            'channel': current_channel,
                            'group_id': current_group,
                            'msg_id': msg_id
                        }, exclude_socket=client_socket)
                        with self.lock:
                            if current_group in self.groups and current_channel in self.groups[current_group]['channels']:
                                self.groups[current_group]['channels'][current_channel].append({
                                    'sender': username, 'content': image_data, 'is_image': True, 'msg_id': msg_id
                                })
                                self._mark_dirty()
                    elif current_channel:
                        self.broadcast_to_channel(current_channel, {
                            'type': 'image_message',
                            'sender': username,
                            'image_data': image_data,
                            'channel': current_channel,
                            'msg_id': msg_id
                        }, exclude_socket=client_socket)
                        with self.lock:
                            self.history[current_channel].append({
                                'sender': username, 'content': image_data, 'is_image': True, 'msg_id': msg_id
                            })
                            if len(self.history[current_channel]) > self.MAX_HISTORY:
                                self.history[current_channel].pop(0)
                            self._mark_dirty()

                elif msg_type == 'file':
                    file_data = msg['file_data']
                    file_name = msg['file_name']
                    file_type = msg.get('file_type', 'unknown')
                    current_channel = self.get_client_channel(username)
                    current_group = self.get_client_group(username)
                    # Используем msg_id от клиента, если есть, иначе генерируем новый
                    msg_id = msg.get('msg_id', str(uuid.uuid4()))
                    if current_group:
                        self.broadcast_to_group_channel(current_group, current_channel, {
                            'type': 'file_message',
                            'sender': username,
                            'file_data': file_data,
                            'file_name': file_name,
                            'file_type': file_type,
                            'channel': current_channel,
                            'group_id': current_group,
                            'msg_id': msg_id
                        }, exclude_socket=client_socket)
                        with self.lock:
                            if current_group in self.groups and current_channel in self.groups[current_group]['channels']:
                                self.groups[current_group]['channels'][current_channel].append({
                                    'sender': username, 'content': file_data, 'is_image': False,
                                    'is_file': True, 'file_name': file_name, 'file_type': file_type, 'msg_id': msg_id
                                })
                                self._mark_dirty()
                    elif current_channel:
                        self.broadcast_to_channel(current_channel, {
                            'type': 'file_message',
                            'sender': username,
                            'file_data': file_data,
                            'file_name': file_name,
                            'file_type': file_type,
                            'channel': current_channel,
                            'msg_id': msg_id
                        }, exclude_socket=client_socket)
                        with self.lock:
                            self.history[current_channel].append({
                                'sender': username, 'content': file_data, 'is_image': False,
                                'is_file': True, 'file_name': file_name, 'file_type': file_type, 'msg_id': msg_id
                            })
                            if len(self.history[current_channel]) > self.MAX_HISTORY:
                                self.history[current_channel].pop(0)
                            self._mark_dirty()

                elif msg_type == 'update_profile':
                    with self.lock:
                        profile = self.profiles.setdefault(username, {})
                        if 'avatar' in msg:
                            profile['avatar'] = msg['avatar']
                        if 'description' in msg:
                            profile['description'] = msg['description']
                        if 'display_name' in msg:
                            profile['display_name'] = msg['display_name']
                        profile_copy = dict(profile)
                        self._mark_dirty()
                    # Уведомляем всех об обновлении профиля
                    self._broadcast_all({
                        'type': 'profile_updated',
                        'username': username,
                        'profile': profile_copy
                    })

                elif msg_type == 'get_profile':
                    target = msg['username']
                    with self.lock:
                        profile = self.profiles.get(target, {'avatar': None, 'description': '', 'display_name': target})
                    self.send_to_client(client_socket, {
                        'type': 'profile_data',
                        'username': target,
                        'profile': profile
                    })

                elif msg_type == 'create_group':
                    group_name = msg['name']
                    with self.lock:
                        self._group_counter += 1
                        gid = f'grp_{self._group_counter}'
                        self.groups[gid] = {
                            'name': group_name,
                            'owner': username,
                            'members': {username},
                            'channels': {'общий': []},
                            'voice_channels': ['Голосовой 1']
                        }
                        self._save_data()
                    self.send_to_client(client_socket, {
                        'type': 'group_created',
                        'group': {
                            'id': gid,
                            'name': group_name,
                            'owner': username,
                            'channels': ['общий'],
                            'voice_channels': ['Голосовой 1'],
                            'members': [username]
                        }
                    })

                elif msg_type == 'invite_to_group':
                    gid = msg['group_id']
                    target = msg['target']
                    with self.lock:
                        if gid in self.groups and username in self.groups[gid]['members']:
                            self.groups[gid]['members'].add(target)
                            self._mark_dirty()
                            group_data = {
                                'id': gid,
                                'name': self.groups[gid]['name'],
                                'owner': self.groups[gid]['owner'],
                                'channels': list(self.groups[gid]['channels'].keys()),
                                'voice_channels': self.groups[gid].get('voice_channels', []),
                                'members': list(self.groups[gid]['members'])
                            }
                    # Уведомляем приглашённого
                    for sock, _, u, _, _, _ in self.clients:
                        if u == target:
                            self.send_to_client(sock, {'type': 'group_invite', 'group': group_data})
                            break
                    # Обновляем членов группы
                    self._broadcast_group_update(gid)

                elif msg_type == 'create_group_channel':
                    gid = msg['group_id']
                    ch_name = msg['channel_name']
                    ch_type = msg.get('channel_type', 'text')
                    with self.lock:
                        if gid in self.groups and self.groups[gid]['owner'] == username:
                            if ch_type == 'voice':
                                self.groups[gid]['voice_channels'].append(ch_name)
                            else:
                                self.groups[gid]['channels'][ch_name] = []
                            self._mark_dirty()
                            group_data = {
                                'id': gid,
                                'name': self.groups[gid]['name'],
                                'owner': self.groups[gid]['owner'],
                                'channels': list(self.groups[gid]['channels'].keys()),
                                'voice_channels': self.groups[gid].get('voice_channels', []),
                                'members': list(self.groups[gid]['members'])
                            }
                    self._broadcast_group_update(gid)

                elif msg_type == 'call_request':
                    to_user = msg['to']
                    with self.lock:
                        caller_profile = self.profiles.get(username, {})
                    with self.lock:
                        for sock, _, u, _, _, _ in self.clients:
                            if u == to_user:
                                self.send_to_client(sock, {
                                    'type': 'call_request',
                                    'from': username,
                                    'ip': msg.get('ip'),
                                    'port': msg.get('port'),
                                    'caller_profile': caller_profile
                                })
                                break

                elif msg_type == 'call_accept':
                    to_user = msg['to']
                    with self.lock:
                        acceptor_profile = self.profiles.get(username, {})
                    with self.lock:
                        for sock, _, u, _, _, _ in self.clients:
                            if u == to_user:
                                self.send_to_client(sock, {
                                    'type': 'call_accept',
                                    'from': username,
                                    'ip': msg['ip'],
                                    'port': msg['port'],
                                    'acceptor_profile': acceptor_profile
                                })
                                break

                elif msg_type == 'join_voice':
                    gid = msg.get('group_id', '')
                    vc_name = msg['vc_name']
                    udp_port = msg.get('udp_port', 0)
                    client_ip = client_socket.getpeername()[0]

                    room_key = (gid, vc_name)
                    with self.lock:
                        if room_key not in self.voice_rooms:
                            self.voice_rooms[room_key] = {'members': {}, 'streamers': set()}
                        self.voice_rooms[room_key]['members'][username] = {
                            'ip': client_ip,
                            'udp_port': udp_port
                        }

                    self._broadcast_voice_members(gid, vc_name)
                    print(f"[VOICE] {username} присоединился к {vc_name} в группе {gid}")

                elif msg_type == 'leave_voice':
                    gid = msg.get('group_id', '')
                    vc_name = msg['vc_name']
                    room_key = (gid, vc_name)

                    with self.lock:
                        if room_key in self.voice_rooms:
                            self.voice_rooms[room_key]['members'].pop(username, None)
                            self.voice_rooms[room_key]['streamers'].discard(username)
                            if not self.voice_rooms[room_key]['members']:
                                del self.voice_rooms[room_key]

                    self._broadcast_voice_members(gid, vc_name)
                    self._broadcast_streamer_list(gid, vc_name)
                    print(f"[VOICE] {username} покинул {vc_name} в группе {gid}")

                elif msg_type == 'get_voice_members':
                    gid = msg.get('group_id', '')
                    vc_name = msg['vc_name']
                    room_key = (gid, vc_name)

                    with self.lock:
                        room = self.voice_rooms.get(room_key, {'members': {}, 'streamers': set()})
                        members = list(room['members'].keys())
                        endpoints = {u: info for u, info in room['members'].items()}
                        audio_states = room.get('audio_states', {})

                    self.send_to_client(client_socket, {
                        'type': 'voice_members',
                        'group_id': gid,
                        'vc_name': vc_name,
                        'members': members,
                        'endpoints': endpoints,
                        'audio_states': audio_states
                    })

                elif msg_type == 'stream_control':
                    gid = msg.get('group_id', '')
                    vc_name = msg['vc_name']
                    action = msg['action']
                    room_key = (gid, vc_name)

                    with self.lock:
                        if room_key in self.voice_rooms:
                            if action == 'start':
                                self.voice_rooms[room_key]['streamers'].add(username)
                            elif action == 'stop':
                                self.voice_rooms[room_key]['streamers'].discard(username)

                    self._broadcast_streamer_list(gid, vc_name)
                    print(f"[STREAM] {username} {'начал' if action == 'start' else 'остановил'} трансляцию в {vc_name}")

                elif msg_type == 'audio_state':
                    gid = msg.get('group_id', '')
                    vc_name = msg['vc_name']
                    mic_muted = msg.get('mic_muted', False)
                    speaker_muted = msg.get('speaker_muted', False)
                    room_key = (gid, vc_name)

                    with self.lock:
                        if room_key in self.voice_rooms and username in self.voice_rooms[room_key]['members']:
                            # Сохраняем состояние аудио для пользователя
                            if 'audio_states' not in self.voice_rooms[room_key]:
                                self.voice_rooms[room_key]['audio_states'] = {}
                            self.voice_rooms[room_key]['audio_states'][username] = {
                                'mic_muted': mic_muted,
                                'speaker_muted': speaker_muted
                            }

                    # Рассылаем обновление состояния всем участникам комнаты
                    self._broadcast_audio_state(gid, vc_name, username, mic_muted, speaker_muted)

                elif msg_type == 'delete_group_channel':
                    gid = msg['group_id']
                    ch_name = msg['channel_name']
                    ch_type = msg.get('channel_type', 'text')
                    with self.lock:
                        if gid in self.groups and self.groups[gid]['owner'] == username:
                            if ch_type == 'voice':
                                if ch_name in self.groups[gid].get('voice_channels', []):
                                    self.groups[gid]['voice_channels'].remove(ch_name)
                                    # Выгоняем всех из голосового канала
                                    room_key = (gid, ch_name)
                                    if room_key in self.voice_rooms:
                                        del self.voice_rooms[room_key]
                            else:
                                self.groups[gid]['channels'].pop(ch_name, None)
                    self._broadcast_group_update(gid)

                elif msg_type == 'delete_message':
                    gid = msg.get('group_id', '')
                    channel = msg.get('channel', '')
                    msg_id = msg.get('msg_id', '')

                    # Удаление доступно всем пользователям
                    can_delete = False
                    with self.lock:
                        if gid and gid in self.groups:
                            # Групповой канал
                            if channel in self.groups[gid]['channels']:
                                for i, m in enumerate(self.groups[gid]['channels'][channel]):
                                    if m.get('msg_id') == msg_id:
                                        can_delete = True
                                        self.groups[gid]['channels'][channel].pop(i)
                                        break
                        elif channel in self.history:
                            # Обычный канал
                            for i, m in enumerate(self.history[channel]):
                                if m.get('msg_id') == msg_id:
                                    can_delete = True
                                    self.history[channel].pop(i)
                                    break
                        if can_delete:
                            self._mark_dirty()

                    if can_delete:
                        # Рассылаем уведомление об удалении всем (включая отправителя)
                        if gid:
                            self._broadcast_to_group(gid, {
                                'type': 'delete_message',
                                'group_id': gid,
                                'channel': channel,
                                'msg_id': msg_id
                            })
                        else:
                            # Рассылаем всем в канале (без исключений)
                            self.broadcast_to_channel(channel, {
                                'type': 'delete_message',
                                'channel': channel,
                                'msg_id': msg_id
                            }, exclude_socket=None)

                elif msg_type == 'delete_dm_message':
                    with_user = msg.get('with', '')
                    msg_id = msg.get('msg_id', '')
                    key = frozenset({username, with_user})

                    can_delete = False
                    with self.lock:
                        if key in self.dm_history:
                            for i, m in enumerate(self.dm_history[key]):
                                if m.get('msg_id') == msg_id:
                                    can_delete = True
                                    self.dm_history[key].pop(i)
                                    break
                        if can_delete:
                            self._mark_dirty()

                    if can_delete:
                        # Уведомляем обоих участников
                        for sock, _, u, _, _, _ in self.clients:
                            if u == with_user or u == username:
                                self.send_to_client(sock, {
                                    'type': 'delete_dm_message',
                                    'with': with_user if u == username else username,
                                    'msg_id': msg_id
                                })

            except json.JSONDecodeError:
                self.send_to_client(client_socket, {'type': 'error', 'message': 'Некорректный формат'})
            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                print(f"[DISCONNECT] Клиент {username} отключился (сетевая ошибка)")
                break
            except Exception as e:
                print(f"Ошибка в handle_client для {username}: {e}")
                traceback.print_exc()
                break

        with self.lock:
            self.clients = [(s, f, u, c, g, ig) for (s, f, u, c, g, ig) in self.clients if u != username]

            # Удаляем пользователя из всех голосовых каналов
            rooms_to_update = []
            for room_key, room in list(self.voice_rooms.items()):
                if username in room['members']:
                    room['members'].pop(username, None)
                    room['streamers'].discard(username)
                    rooms_to_update.append(room_key)
                    if not room['members']:
                        del self.voice_rooms[room_key]

        # Уведомляем об изменениях в голосовых каналах
        for gid, vc_name in rooms_to_update:
            self._broadcast_voice_members(gid, vc_name)
            self._broadcast_streamer_list(gid, vc_name)

        client_socket.close()
        print(f"[DISCONNECT] Клиент {username} отключился")
        self.broadcast_user_list()

    def _handle_dm(self, from_user, to_user, content, is_image=False, is_file=False, file_name=None, file_type=None, msg_id=None):
        key = frozenset({from_user, to_user})
        # Используем msg_id от клиента, если есть, иначе генерируем новый
        if msg_id is None:
            msg_id = str(uuid.uuid4())
        entry = {'sender': from_user, 'content': content, 'is_image': is_image, 'msg_id': msg_id}
        if is_file:
            entry['is_file'] = True
            entry['file_name'] = file_name
            entry['file_type'] = file_type
        with self.lock:
            if key not in self.dm_history:
                self.dm_history[key] = []
            self.dm_history[key].append(entry)
            if len(self.dm_history[key]) > self.MAX_HISTORY:
                self.dm_history[key].pop(0)
            self._mark_dirty()

        if is_file:
            msg_type = 'dm_file_message'
        elif is_image:
            msg_type = 'dm_image_message'
        else:
            msg_type = 'dm_message'

        for sock, _, u, _, _, _ in self.clients:
            if u == to_user:
                payload = {
                    'type': msg_type,
                    'from': from_user,
                    'content': content,
                    'msg_id': msg_id
                }
                if is_file:
                    payload['file_name'] = file_name
                    payload['file_type'] = file_type
                self.send_to_client(sock, payload)
                break
        # Подтверждение отправителю
        for sock, _, u, _, _, _ in self.clients:
            if u == from_user:
                payload = {
                    'type': 'dm_sent',
                    'to': to_user,
                    'content': content,
                    'is_image': is_image,
                    'msg_id': msg_id
                }
                if is_file:
                    payload['is_file'] = True
                    payload['file_name'] = file_name
                    payload['file_type'] = file_type
                self.send_to_client(sock, payload)
                break

    def _broadcast_all(self, message_dict):
        with self.lock:
            for sock, _, _, _, _, _ in self.clients:
                self.send_to_client(sock, message_dict)

    def _broadcast_voice_members(self, gid, vc_name):
        """Рассылает список участников голосового канала всем членам группы"""
        room_key = (gid, vc_name)
        with self.lock:
            room = self.voice_rooms.get(room_key, {'members': {}, 'streamers': set()})
            members = list(room['members'].keys())
            endpoints = {u: info for u, info in room['members'].items()}

            if gid in self.groups:
                group_members = self.groups[gid]['members']
            else:
                group_members = set()

        for sock, _, u, _, _, _ in self.clients:
            if u in group_members:
                self.send_to_client(sock, {
                    'type': 'voice_members',
                    'group_id': gid,
                    'vc_name': vc_name,
                    'members': members,
                    'endpoints': endpoints
                })

    def _broadcast_streamer_list(self, gid, vc_name):
        """Рассылает список стримеров в голосовом канале"""
        room_key = (gid, vc_name)
        with self.lock:
            room = self.voice_rooms.get(room_key, {'members': {}, 'streamers': set()})
            streamers = list(room['streamers'])

            if gid in self.groups:
                group_members = self.groups[gid]['members']
            else:
                group_members = set()

        for sock, _, u, _, _, _ in self.clients:
            if u in group_members:
                self.send_to_client(sock, {
                    'type': 'streamer_list',
                    'group_id': gid,
                    'vc_name': vc_name,
                    'streamers': streamers
                })

    def _broadcast_audio_state(self, gid, vc_name, username, mic_muted, speaker_muted):
        """Рассылает состояние аудио пользователя всем участникам голосового канала"""
        room_key = (gid, vc_name)
        with self.lock:
            if gid in self.groups:
                group_members = self.groups[gid]['members']
            else:
                group_members = set()

        for sock, _, u, _, _, _ in self.clients:
            if u in group_members:
                self.send_to_client(sock, {
                    'type': 'audio_state_update',
                    'group_id': gid,
                    'vc_name': vc_name,
                    'username': username,
                    'mic_muted': mic_muted,
                    'speaker_muted': speaker_muted
                })

    def _broadcast_group_update(self, gid):
        with self.lock:
            if gid not in self.groups:
                return
            g = self.groups[gid]
            group_data = {
                'id': gid,
                'name': g['name'],
                'owner': g['owner'],
                'channels': list(g['channels'].keys()),
                'voice_channels': g.get('voice_channels', []),
                'members': list(g['members'])
            }
            members = set(g['members'])
        for sock, _, u, _, _, _ in self.clients:
            if u in members:
                self.send_to_client(sock, {'type': 'group_updated', 'group': group_data})

    def _broadcast_to_group(self, gid, message_dict):
        """Рассылает сообщение всем членам группы"""
        with self.lock:
            if gid not in self.groups:
                return
            members = set(self.groups[gid]['members'])
        for sock, _, u, _, _, _ in self.clients:
            if u in members:
                self.send_to_client(sock, message_dict)

    def get_client_channel(self, username):
        with self.lock:
            for _, _, u, ch, _, _ in self.clients:
                if u == username:
                    return ch
        return None

    def get_client_group(self, username):
        with self.lock:
            for _, _, u, _, g, _ in self.clients:
                if u == username:
                    return g
        return None

    def set_client_channel(self, username, channel, group_id):
        with self.lock:
            for i, (s, f, u, ch, g, ig) in enumerate(self.clients):
                if u == username:
                    self.clients[i] = (s, f, u, channel, group_id, ig)
                    break

    def broadcast_to_channel(self, channel, message_dict, exclude_socket=None):
        with self.lock:
            for sock, _, u, ch, g, _ in self.clients:
                if ch == channel and g is None and sock != exclude_socket:
                    self.send_to_client(sock, message_dict)

    def broadcast_to_group_channel(self, group_id, channel, message_dict, exclude_socket=None):
        with self.lock:
            for sock, _, u, ch, g, _ in self.clients:
                if g == group_id and ch == channel and sock != exclude_socket:
                    self.send_to_client(sock, message_dict)

    def send_to_client(self, sock, message_dict):
        try:
            sock.sendall((json.dumps(message_dict, ensure_ascii=False) + '\n').encode('utf-8'))
        except (BrokenPipeError, ConnectionResetError, OSError):
            # Клиент отключился, это нормально
            pass
        except Exception as e:
            print(f"[ERROR] Ошибка отправки сообщения клиенту: {e}")

    def broadcast_user_list(self):
        with self.lock:
            users = []
            for (_, _, u, _, _, _) in self.clients:
                profile = self.profiles.get(u, {})
                users.append({
                    'username': u,
                    'display_name': profile.get('display_name', u),
                    'avatar': profile.get('avatar', None),
                    'description': profile.get('description', '')
                })
            userlist_msg = {'type': 'user_list', 'users': users}
            for sock, _, _, _, _, _ in self.clients:
                self.send_to_client(sock, userlist_msg)

    def send_private_message(self, from_user, to_user, text):
        target_socket = None
        with self.lock:
            for sock, _, u, _, _, _ in self.clients:
                if u == to_user:
                    target_socket = sock
                    break
        if target_socket:
            self.send_to_client(target_socket, {
                'type': 'private_message',
                'from': from_user,
                'content': text
            })
            for sock, _, u, _, _, _ in self.clients:
                if u == from_user:
                    self.send_to_client(sock, {
                        'type': 'private_message_sent',
                        'to': to_user,
                        'content': text
                    })
                    break
        else:
            for sock, _, u, _, _, _ in self.clients:
                if u == from_user:
                    self.send_to_client(sock, {'type': 'error', 'message': f'Пользователь {to_user} не найден'})
                    break

    def stop(self):
        print("[INFO] Остановка сервера...")
        self.running = False

        # Отменяем таймер отложенного сохранения, если есть
        if self._save_timer:
            self._save_timer.cancel()

        # Принудительно сохраняем данные перед остановкой
        if self._data_dirty:
            print("[INFO] Сохранение данных перед остановкой...")
            self._save_data()

        if self.server_socket:
            self.server_socket.close()
        if self.udp_socket:
            self.udp_socket.close()
        with self.lock:
            for sock, _, _, _, _, _ in self.clients:
                sock.close()
            self.clients.clear()
            self.voice_rooms.clear()
# Затем в конце файла, в if __name__ == '__main__':
if __name__ == '__main__':
    import os
    import signal
    import threading
    

    # Или берем из переменной окружения
    PORT = int(os.environ.get('PORT', 12345))  # Изменено с 12345 
    HOST = '0.0.0.0'
    
    print(f"🔧 Запускаем на порту: {PORT}")
    
    # Обновите DiscordServer, чтобы он принимал порт
    server = DiscordServer(host=HOST, port=PORT)
    
    # ... остальной код
