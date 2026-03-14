"""QQ channel implementation using botpy SDK."""

import asyncio
import base64
import os
import re
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import QQConfig

try:
    import botpy
    from botpy.message import C2CMessage, GroupMessage

    QQ_AVAILABLE = True
except ImportError:
    QQ_AVAILABLE = False
    botpy = None
    C2CMessage = None
    GroupMessage = None

if TYPE_CHECKING:
    from botpy.message import C2CMessage, GroupMessage


# ============ 本地文件上传扩展 ============

class FileType:
    """文件类型常量"""
    IMAGE = 1
    VIDEO = 2
    VOICE = 3
    FILE = 4


def _get_file_type(file_path: str) -> int:
    """根据文件扩展名判断文件类型"""
    ext = Path(file_path).suffix.lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
        return FileType.IMAGE
    elif ext in ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.flv']:
        return FileType.VIDEO
    elif ext in ['.silk', '.amr', '.wav', '.mp3', '.ogg']:
        return FileType.VOICE
    else:
        return FileType.FILE


def _read_file_base64(file_path: str) -> str:
    """读取本地文件并转为 Base64"""
    with open(file_path, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')


async def _upload_local_file_c2c(
    api,
    openid: str,
    file_path: str,
    file_type: int = None,
    srv_send_msg: bool = False
) -> dict:
    """
    上传本地文件到 C2C (单聊)
    
    Args:
        api: botpy API 对象
        openid: 用户 openid
        file_path: 本地文件路径
        file_type: 文件类型 (1=图片, 2=视频, 3=语音, 4=文件)
        srv_send_msg: 是否直接发送消息
    
    Returns:
        Media 对象，包含 file_info
    """
    if file_type is None:
        file_type = _get_file_type(file_path)
    
    file_data = _read_file_base64(file_path)
    file_name = Path(file_path).name
    
    # 构造请求 payload
    payload = {
        "file_type": file_type,
        "file_data": file_data,
        "srv_send_msg": srv_send_msg,
    }
    
    # file_type=4 (文件) 时需要 file_name
    if file_type == FileType.FILE:
        payload["file_name"] = file_name
    
    # 使用内部 http 客户端发送请求
    from botpy.http import Route
    route = Route("POST", "/v2/users/{openid}/files", openid=openid)
    return await api._http.request(route, json=payload)


async def _upload_local_file_group(
    api,
    group_openid: str,
    file_path: str,
    file_type: int = None,
    srv_send_msg: bool = False
) -> dict:
    """
    上传本地文件到群聊
    
    Args:
        api: botpy API 对象
        group_openid: 群 openid
        file_path: 本地文件路径
        file_type: 文件类型 (1=图片, 2=视频, 3=语音, 4=文件)
        srv_send_msg: 是否直接发送消息
    
    Returns:
        Media 对象，包含 file_info
    """
    if file_type is None:
        file_type = _get_file_type(file_path)
    
    file_data = _read_file_base64(file_path)
    file_name = Path(file_path).name
    
    payload = {
        "file_type": file_type,
        "file_data": file_data,
        "srv_send_msg": srv_send_msg,
    }
    
    if file_type == FileType.FILE:
        payload["file_name"] = file_name
    
    from botpy.http import Route
    route = Route("POST", "/v2/groups/{group_openid}/files", group_openid=group_openid)
    return await api._http.request(route, json=payload)


async def _send_c2c_media_message(
    api,
    openid: str,
    file_info: str,
    msg_id: str = None,
    content: str = None
) -> dict:
    """发送 C2C 富媒体消息"""
    payload = {
        "msg_type": 7,  # 富媒体消息
        "media": {"file_info": file_info},
    }
    if content:
        payload["content"] = content
    if msg_id:
        payload["msg_id"] = msg_id
    
    from botpy.http import Route
    route = Route("POST", "/v2/users/{openid}/messages", openid=openid)
    return await api._http.request(route, json=payload)


async def _send_group_media_message(
    api,
    group_openid: str,
    file_info: str,
    msg_id: str = None,
    content: str = None
) -> dict:
    """发送群富媒体消息"""
    payload = {
        "msg_type": 7,  # 富媒体消息
        "media": {"file_info": file_info},
    }
    if content:
        payload["content"] = content
    if msg_id:
        payload["msg_id"] = msg_id
    
    from botpy.http import Route
    route = Route("POST", "/v2/groups/{group_openid}/messages", group_openid=group_openid)
    return await api._http.request(route, json=payload)


# ============ 媒体标签解析 ============

MEDIA_TAG_REGEX = re.compile(
    r'<(qqimg|qqvoice|qqvideo|qqfile)>([^<>]+)</(?:qqimg|qqvoice|qqvideo|qqfile|img)>',
    re.IGNORECASE
)


def _parse_media_tags(text: str) -> list:
    """
    解析消息中的媒体标签
    
    Returns:
        发送队列，每个元素是 (type, content) 元组
        type: "text" | "image" | "voice" | "video" | "file"
        content: 文本内容或文件路径/URL
    """
    send_queue = []
    last_index = 0
    
    for match in MEDIA_TAG_REGEX.finditer(text):
        # 添加标签前的文本
        text_before = text[last_index:match.start()].strip()
        if text_before:
            send_queue.append(("text", text_before))
        
        tag_name = match.group(1).lower()
        media_path = match.group(2).strip()
        
        if tag_name == "qqvoice":
            send_queue.append(("voice", media_path))
        elif tag_name == "qqvideo":
            send_queue.append(("video", media_path))
        elif tag_name == "qqfile":
            send_queue.append(("file", media_path))
        else:  # qqimg
            send_queue.append(("image", media_path))
        
        last_index = match.end()
    
    # 添加最后一个标签后的文本
    text_after = text[last_index:].strip()
    if text_after:
        send_queue.append(("text", text_after))
    
    return send_queue


def _is_local_path(path: str) -> bool:
    """判断是否为本地文件路径"""
    return not path.startswith(('http://', 'https://', 'data:'))


def _is_url(path: str) -> bool:
    """判断是否为 URL"""
    return path.startswith(('http://', 'https://'))


# ============ Bot 类定义 ============

def _make_bot_class(channel: "QQChannel") -> "type[botpy.Client]":
    """Create a botpy Client subclass bound to the given channel."""
    intents = botpy.Intents(public_messages=True, direct_message=True)

    class _Bot(botpy.Client):
        def __init__(self):
            # Disable botpy's file log — nanobot uses loguru; default "botpy.log" fails on read-only fs
            super().__init__(intents=intents, ext_handlers=False)

        async def on_ready(self):
            logger.info("QQ bot ready: {}", self.robot.name)

        async def on_c2c_message_create(self, message: "C2CMessage"):
            await channel._on_message(message, is_group=False)

        async def on_group_at_message_create(self, message: "GroupMessage"):
            await channel._on_message(message, is_group=True)

        async def on_direct_message_create(self, message):
            await channel._on_message(message, is_group=False)

    return _Bot


class QQChannel(BaseChannel):
    """QQ channel using botpy SDK with WebSocket connection."""

    name = "qq"
    display_name = "QQ"

    def __init__(self, config: QQConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: QQConfig = config
        self._client: "botpy.Client | None" = None
        self._processed_ids: deque = deque(maxlen=1000)
        self._msg_seq: int = 1  # 消息序列号，避免被 QQ API 去重
        self._chat_type_cache: dict[str, str] = {}

    async def start(self) -> None:
        """Start the QQ bot."""
        if not QQ_AVAILABLE:
            logger.error("QQ SDK not installed. Run: pip install qq-botpy")
            return

        if not self.config.app_id or not self.config.secret:
            logger.error("QQ app_id and secret not configured")
            return

        self._running = True
        BotClass = _make_bot_class(self)
        self._client = BotClass()
        logger.info("QQ bot started (C2C & Group supported)")
        await self._run_bot()

    async def _run_bot(self) -> None:
        """Run the bot connection with auto-reconnect."""
        while self._running:
            try:
                await self._client.start(appid=self.config.app_id, secret=self.config.secret)
            except Exception as e:
                logger.warning("QQ bot error: {}", e)
            if self._running:
                logger.info("Reconnecting QQ bot in 5 seconds...")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop the QQ bot."""
        self._running = False
        if self._client:
            try:
                await self._client.close()
            except Exception:
                pass
        logger.info("QQ bot stopped")

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through QQ."""
        if not self._client:
            logger.warning("QQ client not initialized")
            return

        try:
            msg_id = msg.metadata.get("message_id")
            msg_type = self._chat_type_cache.get(msg.chat_id, "c2c")
            
            # 先处理 msg.media 附件列表
            if msg.media:
                for file_path in msg.media:
                    try:
                        file_type = _get_file_type(file_path)
                        if file_type == FileType.IMAGE:
                            await self._send_image(msg.chat_id, file_path, msg_id, msg_type)
                        elif file_type == FileType.VIDEO:
                            await self._send_video(msg.chat_id, file_path, msg_id, msg_type)
                        elif file_type == FileType.VOICE:
                            await self._send_voice(msg.chat_id, file_path, msg_id, msg_type)
                        else:
                            await self._send_file(msg.chat_id, file_path, msg_id, msg_type)
                    except Exception as e:
                        logger.error("Error sending media {}: {}", file_path, e)
            
            # 解析消息内容中的媒体标签
            send_queue = _parse_media_tags(msg.content)
            
            if not send_queue:
                # 没有媒体标签，发送普通文本
                if msg.content:
                    await self._send_text_message(msg.chat_id, msg.content, msg_id, msg_type)
                return
            
            # 按顺序发送队列中的内容
            for item_type, item_content in send_queue:
                try:
                    if item_type == "text":
                        await self._send_text_message(msg.chat_id, item_content, msg_id, msg_type)
                    elif item_type == "image":
                        await self._send_image(msg.chat_id, item_content, msg_id, msg_type)
                    elif item_type == "voice":
                        await self._send_voice(msg.chat_id, item_content, msg_id, msg_type)
                    elif item_type == "video":
                        await self._send_video(msg.chat_id, item_content, msg_id, msg_type)
                    elif item_type == "file":
                        await self._send_file(msg.chat_id, item_content, msg_id, msg_type)
                except Exception as e:
                    logger.error("Error sending {}: {}", item_type, e)
                    # 继续发送队列中的其他内容
                    
        except Exception as e:
            logger.error("Error sending QQ message: {}", e)

    async def _send_text_message(self, chat_id: str, content: str, msg_id: str, msg_type: str) -> None:
        """发送文本消息"""
        self._msg_seq += 1
        if msg_type == "group":
            await self._client.api.post_group_message(
                group_openid=chat_id,
                msg_type=2,
                content=content,
                markdown={"content": content},
                msg_id=msg_id,
                msg_seq=self._msg_seq,
            )
        else:
            await self._client.api.post_c2c_message(
                openid=chat_id,
                msg_type=2,
                content=content,
                markdown={"content": content},
                msg_id=msg_id,
                msg_seq=self._msg_seq,
            )

    async def _send_image(self, chat_id: str, image_path: str, msg_id: str, msg_type: str) -> None:
        """发送图片（支持本地文件和URL）"""
        if _is_url(image_path):
            # 使用 botpy 原生的 URL 方式
            if msg_type == "group":
                media = await self._client.api.post_group_file(
                    group_openid=chat_id,
                    file_type=FileType.IMAGE,
                    url=image_path
                )
                await self._client.api.post_group_message(
                    group_openid=chat_id,
                    msg_type=7,
                    media=media,
                    msg_id=msg_id,
                    msg_seq=self._msg_seq,
                )
            else:
                media = await self._client.api.post_c2c_file(
                    openid=chat_id,
                    file_type=FileType.IMAGE,
                    url=image_path
                )
                await self._client.api.post_c2c_message(
                    openid=chat_id,
                    msg_type=7,
                    media=media,
                    msg_id=msg_id,
                    msg_seq=self._msg_seq,
                )
        else:
            # 本地文件 - 使用扩展方法
            if msg_type == "group":
                media = await _upload_local_file_group(
                    self._client.api, chat_id, image_path, FileType.IMAGE
                )
                await _send_group_media_message(
                    self._client.api, chat_id, media["file_info"], msg_id
                )
            else:
                media = await _upload_local_file_c2c(
                    self._client.api, chat_id, image_path, FileType.IMAGE
                )
                await _send_c2c_media_message(
                    self._client.api, chat_id, media["file_info"], msg_id
                )

    async def _send_voice(self, chat_id: str, voice_path: str, msg_id: str, msg_type: str) -> None:
        """发送语音（支持本地文件和URL）"""
        if _is_url(voice_path):
            if msg_type == "group":
                media = await self._client.api.post_group_file(
                    group_openid=chat_id,
                    file_type=FileType.VOICE,
                    url=voice_path
                )
                await self._client.api.post_group_message(
                    group_openid=chat_id,
                    msg_type=7,
                    media=media,
                    msg_id=msg_id,
                    msg_seq=self._msg_seq,
                )
            else:
                media = await self._client.api.post_c2c_file(
                    openid=chat_id,
                    file_type=FileType.VOICE,
                    url=voice_path
                )
                await self._client.api.post_c2c_message(
                    openid=chat_id,
                    msg_type=7,
                    media=media,
                    msg_id=msg_id,
                    msg_seq=self._msg_seq,
                )
        else:
            # 本地文件
            if msg_type == "group":
                media = await _upload_local_file_group(
                    self._client.api, chat_id, voice_path, FileType.VOICE
                )
                await _send_group_media_message(
                    self._client.api, chat_id, media["file_info"], msg_id
                )
            else:
                media = await _upload_local_file_c2c(
                    self._client.api, chat_id, voice_path, FileType.VOICE
                )
                await _send_c2c_media_message(
                    self._client.api, chat_id, media["file_info"], msg_id
                )

    async def _send_video(self, chat_id: str, video_path: str, msg_id: str, msg_type: str) -> None:
        """发送视频（支持本地文件和URL）"""
        if _is_url(video_path):
            if msg_type == "group":
                media = await self._client.api.post_group_file(
                    group_openid=chat_id,
                    file_type=FileType.VIDEO,
                    url=video_path
                )
                await self._client.api.post_group_message(
                    group_openid=chat_id,
                    msg_type=7,
                    media=media,
                    msg_id=msg_id,
                    msg_seq=self._msg_seq,
                )
            else:
                media = await self._client.api.post_c2c_file(
                    openid=chat_id,
                    file_type=FileType.VIDEO,
                    url=video_path
                )
                await self._client.api.post_c2c_message(
                    openid=chat_id,
                    msg_type=7,
                    media=media,
                    msg_id=msg_id,
                    msg_seq=self._msg_seq,
                )
        else:
            # 本地文件
            if msg_type == "group":
                media = await _upload_local_file_group(
                    self._client.api, chat_id, video_path, FileType.VIDEO
                )
                await _send_group_media_message(
                    self._client.api, chat_id, media["file_info"], msg_id
                )
            else:
                media = await _upload_local_file_c2c(
                    self._client.api, chat_id, video_path, FileType.VIDEO
                )
                await _send_c2c_media_message(
                    self._client.api, chat_id, media["file_info"], msg_id
                )

    async def _send_file(self, chat_id: str, file_path: str, msg_id: str, msg_type: str) -> None:
        """发送文件（支持本地文件和URL）"""
        if _is_url(file_path):
            if msg_type == "group":
                media = await self._client.api.post_group_file(
                    group_openid=chat_id,
                    file_type=FileType.FILE,
                    url=file_path
                )
                await self._client.api.post_group_message(
                    group_openid=chat_id,
                    msg_type=7,
                    media=media,
                    msg_id=msg_id,
                    msg_seq=self._msg_seq,
                )
            else:
                media = await self._client.api.post_c2c_file(
                    openid=chat_id,
                    file_type=FileType.FILE,
                    url=file_path
                )
                await self._client.api.post_c2c_message(
                    openid=chat_id,
                    msg_type=7,
                    media=media,
                    msg_id=msg_id,
                    msg_seq=self._msg_seq,
                )
        else:
            # 本地文件
            if msg_type == "group":
                media = await _upload_local_file_group(
                    self._client.api, chat_id, file_path, FileType.FILE
                )
                await _send_group_media_message(
                    self._client.api, chat_id, media["file_info"], msg_id
                )
            else:
                media = await _upload_local_file_c2c(
                    self._client.api, chat_id, file_path, FileType.FILE
                )
                await _send_c2c_media_message(
                    self._client.api, chat_id, media["file_info"], msg_id
                )

    async def _on_message(self, data: "C2CMessage | GroupMessage", is_group: bool = False) -> None:
        """Handle incoming message from QQ."""
        try:
            # Dedup by message ID
            if data.id in self._processed_ids:
                return
            self._processed_ids.append(data.id)

            content = (data.content or "").strip()

            # 提取图片附件并下载到本地
            media_paths = []
            if hasattr(data, 'attachments') and data.attachments:
                for attachment in data.attachments:
                    content_type = getattr(attachment, 'content_type', '') or ''
                    url = getattr(attachment, 'url', None)
                    filename = getattr(attachment, 'filename', None) or 'image'
                    
                    if url and content_type.startswith('image/'):
                        # 下载图片到本地
                        local_path = await self._download_image(url, filename, content_type)
                        if local_path:
                            media_paths.append(local_path)
                            logger.debug("QQ image downloaded: {} -> {}", url, local_path)

            # 如果既没有文本也没有图片，跳过
            if not content and not media_paths:
                return

            if is_group:
                chat_id = data.group_openid
                user_id = data.author.member_openid
                self._chat_type_cache[chat_id] = "group"
            else:
                chat_id = str(getattr(data.author, 'id', None) or getattr(data.author, 'user_openid', 'unknown'))
                user_id = chat_id
                self._chat_type_cache[chat_id] = "c2c"

            await self._handle_message(
                sender_id=user_id,
                chat_id=chat_id,
                content=content,
                media=media_paths if media_paths else None,
                metadata={"message_id": data.id},
            )
        except Exception:
            logger.exception("Error handling QQ message")

    async def _download_image(self, url: str, filename: str, content_type: str) -> str | None:
        """Download image from URL to local media directory."""
        try:
            # 确定文件扩展名
            ext_map = {
                'image/png': '.png',
                'image/jpeg': '.jpg',
                'image/gif': '.gif',
                'image/webp': '.webp',
                'image/bmp': '.bmp',
            }
            ext = ext_map.get(content_type, Path(filename).suffix or '.jpg')
            
            # 创建媒体目录
            media_dir = get_media_dir("qq")
            
            # 使用 URL 的一部分作为文件名，避免冲突
            import hashlib
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            file_path = media_dir / f"{url_hash}{ext}"
            
            # 如果文件已存在，直接返回
            if file_path.exists():
                return str(file_path)
            
            # 下载图片
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                file_path.write_bytes(response.content)
            
            return str(file_path)
        except Exception as e:
            logger.warning("Failed to download QQ image {}: {}", url, e)
            return None