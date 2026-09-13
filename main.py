# -*- coding: utf-8 -*-
"""wzry_watcher —— 王者荣耀官方动态订阅插件

数据源：
- 列表：王者荣耀官网新闻中心 cmc/cross 接口（apps.game.qq.com/cmc/cross）
  签名算法取自官网前端 newsindex.js（token 固定，md5(token+source+serviceId+timestamp)）。
- 正文：wmp/v3.1/public/searchNews.php（详情页 fillnewsgicp/v1.2.js 所用的公开接口，
  返回 `var searchObj={...};`，msg.sContent 为完整正文 HTML，无需登录态）。
"""
import asyncio
import hashlib
import html as html_lib
import json
import os
import re
import time

import httpx

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools, register

API_URL = "https://apps.game.qq.com/cmc/cross"
CONTENT_API = "https://apps.game.qq.com/wmp/v3.1/public/searchNews.php"
SIGN_TOKEN = "234ce0aef3020cb83883877b64869"
SERVICE_ID = 18
# 文本公告详情页（newsdetail）与视频详情页（v/detail）模板不同，必须按类型区分
DETAIL_URL_TPL_NEWS = "https://pvp.qq.com/web201706/newsdetail.shtml?tid={}"
DETAIL_URL_TPL_VIDEO = "https://pvp.qq.com/v/detail.shtml?G_Biz=18&tid={}"
# 单条消息正文分片长度（QQ 长消息会被平台截断/风控，插件内自行分段）
CHUNK_SIZE = 1800
# 正文长图渲染参数（880px 宽：QQ 手机端以聊天宽度显示，宽度越小实际观感字号越大）
IMG_WIDTH = 880
IMG_PAD = 44

try:
    from PIL import Image, ImageDraw, ImageFont

    PIL_OK = True
except ImportError:
    PIL_OK = False

# 中文字体探测：插件自带 Noto CJK 优先，其次系统字体（最后两行为本地 Windows 测试用）
FONT_CANDIDATES_REGULAR = [
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", "fonts", "NotoSansCJKsc-Regular.otf"),
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    "C:/Windows/Fonts/msyh.ttc",
    "/System/Library/Fonts/PingFang.ttc",
]
FONT_CANDIDATES_BOLD = [
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", "fonts", "NotoSansCJKsc-Bold.otf"),
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
    "C:/Windows/Fonts/msyhbd.ttc",
    "/System/Library/Fonts/PingFang.ttc",
]
_FONT_CACHE = {}

# 发布包不捆绑字体（超市场 16MB 限制）；无中文字体的主机首次渲染时自动下载
FONT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", "fonts")
FONT_URLS = {
    "NotoSansCJKsc-Regular.otf": "https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/SimplifiedChinese/NotoSansCJKsc-Regular.otf",
    "NotoSansCJKsc-Bold.otf": "https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/SimplifiedChinese/NotoSansCJKsc-Bold.otf",
}
_font_download_done = False


def ensure_fonts_downloaded() -> bool:
    """检查本机候选字体；全都没有时从 Noto CJK 官方仓库下载一次。返回字体是否可用。"""
    global _font_download_done
    if _font_download_done:
        return any(os.path.exists(p) for p in FONT_CANDIDATES_REGULAR)
    _font_download_done = True
    if any(os.path.exists(p) for p in FONT_CANDIDATES_REGULAR):
        return True
    for fname, url in FONT_URLS.items():
        target = os.path.join(FONT_DIR, fname)
        if os.path.exists(target):
            continue
        try:
            import urllib.request
            os.makedirs(FONT_DIR, exist_ok=True)
            tmp = target + ".tmp"
            logger.info(f"[wzry_watcher] 本机未找到中文字体，正在下载 {fname} …")
            urllib.request.urlretrieve(url, tmp)
            os.replace(tmp, target)
            logger.info(f"[wzry_watcher] 字体下载完成: {fname}")
        except Exception as e:
            logger.warning(f"[wzry_watcher] 字体下载失败（将回退文本输出）: {e}")
    return any(os.path.exists(p) for p in FONT_CANDIDATES_REGULAR)


def _load_font(size: int, bold: bool = False):
    """加载中文字体（带缓存），找不到返回 None。"""
    if not PIL_OK:
        return None
    weight = "Bold" if bold else "Regular"
    key = (weight, size)
    if key in _FONT_CACHE:
        return _FONT_CACHE[key]
    for p in (FONT_CANDIDATES_BOLD if bold else FONT_CANDIDATES_REGULAR) + FONT_CANDIDATES_REGULAR:
        if os.path.exists(p):
            try:
                _FONT_CACHE[key] = ImageFont.truetype(p, size)
                return _FONT_CACHE[key]
            except Exception:
                continue
    _FONT_CACHE[key] = None
    return None

CHANNEL_MAP = {
    "热点": 1760,
    "新闻": 1761,
    "公告": 1762,
    "活动": 1763,
    "赛事": 1764,
}
DEFAULT_KEYWORDS = ["更新", "公告", "活动", "福利", "新皮肤", "版本"]
SEEN_MAX = 500


@register("wzry_watcher", "wei", "监听王者荣耀官方网站（pvp.qq.com）的公告和活动更新，支持五频道全量推送", "1.4.0", "")
class WzryWatcher(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config
        self.data_path = os.path.join(os.getcwd(), "data", "wzry_watcher.json")
        self.data = self._load_data()
        self._task = None

    # ==================== 数据持久化 ====================

    def _load_data(self):
        default = {"seen": [], "umo_map": {}}
        try:
            if os.path.exists(self.data_path):
                with open(self.data_path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"[wzry_watcher] 加载数据失败: {e}")
        return default

    def _save_data(self):
        try:
            os.makedirs(os.path.dirname(self.data_path), exist_ok=True)
            tmp = self.data_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.data_path)
        except Exception as e:
            logger.warning(f"[wzry_watcher] 保存数据失败: {e}")

    def _record_umo(self, group_id, umo):
        if umo and self.data["umo_map"].get(str(group_id)) != umo:
            self.data["umo_map"][str(group_id)] = umo
            self._save_data()

    # ==================== 拉取与过滤 ====================

    async def _fetch_news(self, chanid: int, limit: int = 20):
        """拉取指定频道的新闻列表，返回 [{title, time, id, url, tag}]"""
        ts = int(time.time())
        sign = hashlib.md5(
            f"{SIGN_TOKEN}web_pc{SERVICE_ID}{ts}".encode()
        ).hexdigest()
        params = {
            "serviceId": SERVICE_ID,
            "filter": "channel",
            "sortby": "sIdxTime",
            "source": "web_pc",
            "limit": limit,
            "logic": "or",
            "typeids": "1,2",
            "chanid": chanid,
            "start": 0,
            "withtop": "yes",
            "exclusiveChannel": 4,
            "exclusiveChannelSign": sign,
            "time": ts,
        }
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(API_URL, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        items = ((data.get("data") or {}).get("items")) or []
        result = []
        for it in items:
            nid = it.get("iId") or it.get("iNewsId")
            if not nid:
                continue
            is_video = bool(it.get("sVID") or it.get("iVideoId"))
            url_tpl = DETAIL_URL_TPL_VIDEO if is_video else DETAIL_URL_TPL_NEWS
            result.append(
                {
                    "title": (it.get("sTitle") or "").strip(),
                    "time": it.get("sIdxTime") or "",
                    "id": str(nid),
                    "url": url_tpl.format(nid),
                    "tag": it.get("sTagInfo") or "",
                    "img": it.get("sIMGNew") or "",
                    "is_video": is_video,
                }
            )
        return result

    def _match(self, news: dict, keywords: list) -> bool:
        hay = (news["title"] + " " + news["tag"]).lower()
        return any(kw.strip().lower() in hay for kw in keywords if kw and kw.strip())

    # ==================== 正文全文 ====================

    async def _fetch_content(self, news_id: str):
        """拉取公告正文全文（wmp searchNews.php，官网详情页同源接口）。

        返回 {"title": str, "time": str, "content": str(纯文本)}，失败返回 None。
        """
        params = {"p0": SERVICE_ID, "source": "web_pc", "id": str(news_id)}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://pvp.qq.com/",
        }
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(CONTENT_API, params=params, headers=headers)
            resp.raise_for_status()
            text = resp.text
        start = text.find("{")
        if start < 0:
            return None
        try:
            obj, _ = json.JSONDecoder().raw_decode(text, start)
        except ValueError as e:
            logger.warning(f"[wzry_watcher] 正文响应解析失败: {e}")
            return None
        if obj.get("status") != 0:
            return None
        msg = obj.get("msg") or {}
        content_html = msg.get("sContent") or ""
        if not content_html:
            return None
        return {
            "title": (msg.get("sTitle") or "").strip(),
            "time": (msg.get("sCreated") or "")[:16],
            "content": self._html_to_text(content_html),
        }

    @staticmethod
    def _html_to_text(h: str) -> str:
        """官网正文 HTML 转纯文本：去标签/解实体，段落间空行，忽略图片。"""
        if not h:
            return ""
        h = re.sub(r"(?is)<(script|style).*?</\1>", "", h)
        h = re.sub(r"(?i)<br\s*/?>", "\n", h)
        h = re.sub(r"(?i)</(p|div|tr|h[1-6]|li)>", "\n", h)
        h = re.sub(r"(?i)</td>", " ", h)
        h = re.sub(r"(?is)<img[^>]*>", "", h)
        h = re.sub(r"(?s)<[^>]+>", "", h)
        h = html_lib.unescape(h).replace("\u00a0", " ").replace("\u200b", "")
        lines = [re.sub(r"[ \t]{2,}", " ", ln).strip() for ln in h.split("\n")]
        out, blank = [], False
        for ln in lines:
            if not ln:
                blank = True
                continue
            if blank and out:
                out.append("")
            blank = False
            out.append(ln)
        return "\n".join(out).strip()

    @staticmethod
    def _split_chunks(text: str, size: int = CHUNK_SIZE) -> list:
        """按换行边界把长文本切成多条消息，避免平台截断。"""
        chunks = []
        rest = text
        while rest:
            if len(rest) <= size:
                chunks.append(rest)
                break
            cut = rest.rfind("\n", 0, size)
            if cut < size // 2:
                cut = size
            chunks.append(rest[:cut].rstrip())
            rest = rest[cut:].lstrip("\n")
        return chunks

    # ==================== 正文长图渲染 ====================

    @staticmethod
    def _strip_unrenderable(text: str) -> str:
        """去掉 CJK 字体渲染不了的字符（emoji、变体选择符等），避免豆腐块。"""
        out = []
        for ch in text:
            o = ord(ch)
            if o > 0xFFFF or o in (0xFE0E, 0xFE0F, 0x200D):
                continue
            out.append(ch)
        return "".join(out)

    @staticmethod
    def _wrap_text(text: str, font, max_w: int) -> list:
        """按像素宽度逐字换行（CJK 场景），保留段落结构。"""
        lines = []
        cur = ""
        for ch in text:
            if cur and font.getlength(cur + ch) > max_w:
                lines.append(cur)
                cur = ch
            else:
                cur += ch
        if cur:
            lines.append(cur)
        return lines

    @staticmethod
    def _wrap_with_lead(rest: str, font, first_w: int, max_w: int) -> list:
        """带前置引导段（如【标签】+值同行）的换行：首行可用宽 first_w，后续 max_w。"""
        lines = []
        cur = ""
        width = first_w
        for ch in rest:
            if cur and font.getlength(cur + ch) > width:
                lines.append(cur)
                cur = ch
                width = max_w
            else:
                cur += ch
        if cur:
            lines.append(cur)
        return lines

    @classmethod
    def _render_raw_image(cls, title: str, meta: str, body: str, footer: str, out_path: str):
        """全文长图（移动端优先字号，【小节】标题高亮），成功返回路径，失败返回 None。"""
        if not PIL_OK:
            return None
        f_tag = _load_font(26)
        f_title = _load_font(38, bold=True)
        f_body = _load_font(31)
        f_lead = _load_font(31, bold=True)
        f_hdr = _load_font(33, bold=True)
        f_foot = _load_font(24)
        if not all((f_tag, f_title, f_body, f_lead, f_hdr, f_foot)):
            return None
        try:
            title = cls._strip_unrenderable(title)
            meta = cls._strip_unrenderable(meta)
            footer = cls._strip_unrenderable(footer)
            body = cls._strip_unrenderable(body)

            W, PAD = IMG_WIDTH, IMG_PAD
            max_w = W - PAD * 2
            C_TITLE, C_BODY, C_META, C_LINE = (17, 17, 17), (45, 45, 45), (150, 150, 150), (232, 232, 232)
            ACCENT = (28, 84, 223)
            LH, GAP = 50, 18        # 正文行高/段间距
            HDR_LH, HDR_GAP = 54, 14  # 小节标题行高/额外上间距

            # 正文条目分类：独立【小节】标题行高亮；【标签】值同行 以引导段渲染
            items = []  # (kind, lead, lines)
            for p in body.split("\n"):
                p = p.strip()
                if not p:
                    continue
                m = re.match(r"^(【[^】]{1,20}】)(.*)$", p)
                if m and not m.group(2).strip():
                    items.append(("hdr", m.group(1), [m.group(1)]))
                elif m:
                    lead, rest = m.group(1), m.group(2).strip()
                    lead_w = f_lead.getlength(lead) + 6
                    lines = cls._wrap_with_lead(rest, f_body, max(200, max_w - lead_w), max_w)
                    items.append(("txt", lead, lines))
                else:
                    items.append(("txt", "", cls._wrap_text(p, f_body, max_w)))

            title_lines = cls._wrap_text(title, f_title, max_w)
            footer_lines = cls._wrap_text(footer, f_foot, max_w)

            header_h = PAD + 26 + 20 + len(title_lines) * 56 + 12 + 26 + 14 + 2 + 22
            body_h = 0
            for i, (kind, lead, lines) in enumerate(items):
                body_h += (HDR_GAP if i else 0) + HDR_LH + 8 if kind == "hdr" else (GAP if i else 0) + len(lines) * LH
            foot_h = 26 + 2 + 16 + len(footer_lines) * 32 + PAD
            H = header_h + body_h + foot_h

            img = Image.new("RGB", (W, H), (255, 255, 255))
            d = ImageDraw.Draw(img)
            y = PAD
            d.text((PAD, y), "王者荣耀官网公告", font=f_tag, fill=C_META)
            t_w = f_tag.getlength(meta)
            d.text((W - PAD - t_w, y), meta, font=f_tag, fill=C_META)
            y += 26 + 20
            for ln in title_lines:
                d.text((PAD, y), ln, font=f_title, fill=C_TITLE)
                y += 56
            y += 12
            d.text((PAD, y), "以下为官网原文直出，未经 AI 加工", font=f_tag, fill=C_META)
            y += 26 + 14
            d.line((PAD, y, W - PAD, y), fill=C_LINE, width=2)
            y += 2 + 22

            for i, (kind, lead, lines) in enumerate(items):
                if kind == "hdr":
                    y += HDR_GAP if i else 0
                    d.text((PAD, y), lines[0], font=f_hdr, fill=ACCENT)
                    y += HDR_LH + 8
                else:
                    y += GAP if i else 0
                    if lead:
                        d.text((PAD, y), lead, font=f_lead, fill=ACCENT)
                        x0 = PAD + f_lead.getlength(lead) + 6
                    else:
                        x0 = PAD
                    for j, ln in enumerate(lines):
                        d.text((x0 if j == 0 and lead else PAD, y), ln, font=f_body, fill=C_BODY)
                        y += LH

            y += 26
            d.line((PAD, y, W - PAD, y), fill=C_LINE, width=2)
            y += 2 + 16
            for ln in footer_lines:
                d.text((PAD, y), ln, font=f_foot, fill=C_META)
                y += 32

            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            img.save(out_path, "PNG")
            return out_path
        except Exception as e:
            logger.warning(f"[wzry_watcher] 渲染图片失败: {e}")
            return None

    # ==================== 速览卡（结构化提取，非 AI 总结） ====================

    FACT_KEYS = ("更新时间", "维护时间", "更新方式", "更新范围", "维护补偿", "补偿", "生效时间", "回滚时间")

    @classmethod
    def _extract_summary(cls, body: str) -> dict:
        """从官方正文的【小节】结构提取关键信息与小节目录（纯规则，不经模型）。"""
        facts = []
        seen = set()
        sections = []
        skins = []
        fixes = 0
        for line in body.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("修复了"):
                fixes += 1
            for m in re.finditer(r"([\u4e00-\u9fa5A-Za-z0-9·]{2,10}-[\u4e00-\u9fa5A-Za-z0-9·]{2,14}?)皮肤上架", line):
                name = m.group(1)
                # 前缀去重：「X-Y全新传说限定」与「X-Y」视为同一款，保留短名
                for i, s in enumerate(skins):
                    if s.startswith(name):
                        skins[i] = name
                        name = None
                        break
                    if name.startswith(s):
                        name = None
                        break
                if name:
                    skins.append(name)
            m = re.match(r"^【([^】]{1,20})】\s*(.*)$", line)
            if not m:
                if "本次更新大小" in line and "更新大小" not in seen:
                    seen.add("更新大小")
                    v = re.split(r"[。；;]", line)[0].strip()
                    if v:
                        v = re.sub(r"^本次更新大小", "", v)[:40]
                        facts.append(("更新大小", v))
                continue
            k, v = m.group(1), m.group(2).strip()
            if v and k in cls.FACT_KEYS and k not in seen:
                seen.add(k)
                facts.append((k, re.split(r"[。；;]", v)[0].strip()[:40]))
            elif k not in sections:
                sections.append(k)
        if fixes:
            facts.append(("问题修复", f"{fixes} 项"))
        return {
            "facts": facts[:7],
            "sections": sections[:12],
            "sections_more": max(0, len(sections) - 12),
            "skins": skins[:5],
            "skins_more": max(0, len(skins) - 5),
        }

    @classmethod
    def _render_summary_image(cls, title: str, meta: str, summary: dict, url: str, out_path: str):
        """渲染公告速览卡 PNG，成功返回路径，失败返回 None。"""
        if not PIL_OK:
            return None
        facts = summary.get("facts") or []
        sections = summary.get("sections") or []
        sections_more = summary.get("sections_more") or 0
        skins = summary.get("skins") or []
        skins_more = summary.get("skins_more") or 0
        f_tag = _load_font(26)
        f_title = _load_font(40, bold=True)
        f_sec = _load_font(30, bold=True)
        f_row = _load_font(28)
        f_row_b = _load_font(28, bold=True)
        f_foot = _load_font(24)
        if not all((f_tag, f_title, f_sec, f_row, f_row_b, f_foot)):
            return None
        try:
            title = cls._strip_unrenderable(title)
            meta = cls._strip_unrenderable(meta)
            W, PAD = IMG_WIDTH, IMG_PAD
            max_w = W - PAD * 2
            C_TITLE, C_BODY, C_META, C_LINE = (17, 17, 17), (45, 45, 45), (150, 150, 150), (232, 232, 232)
            ACCENT, GOLD = (28, 84, 223), (196, 154, 84)

            title_lines = cls._wrap_text(title, f_title, max_w)
            url_lines = cls._wrap_text(url, f_foot, max_w)

            # 高度与绘制同步累加，公式保持一致
            h = 12 + 32 + 26 + 18 + len(title_lines) * 58 + 12 + 30
            h += (22 + 30 + 12 + len(facts) * 44) if facts else 0
            h += (26 + 30 + 12 + len(skins) * 40 + (40 if skins_more else 0)) if skins else 0
            h += (26 + 30 + 12 + len(sections) * 40 + (40 if sections_more else 0)) if sections else 0
            h += 24 + 2 + 18 + 26 + 10 + len(url_lines) * 32 + PAD

            img = Image.new("RGB", (W, h), (255, 255, 255))
            d = ImageDraw.Draw(img)
            d.rectangle((0, 0, W, 12), fill=GOLD)
            y = 44
            d.text((PAD, y), "📌 王者荣耀公告速览", font=f_tag, fill=C_META)
            y += 26 + 18
            for ln in title_lines:
                d.text((PAD, y), ln, font=f_title, fill=C_TITLE)
                y += 58
            y += 12
            d.text((PAD, y), f"🕐 {meta}｜官网发布", font=f_tag, fill=C_META)
            y += 30

            if facts:
                y += 22
                d.text((PAD, y), "⚡ 关键信息", font=f_sec, fill=C_TITLE)
                y += 30 + 12
                for k, v in facts:
                    kv = f"{k}："
                    d.text((PAD, y), kv, font=f_row_b, fill=ACCENT)
                    x0 = PAD + f_row_b.getlength(kv)
                    avail = max_w - (x0 - PAD) - 30
                    v2 = v
                    while v2 and f_row.getlength(v2) > avail:
                        v2 = v2[:-1]
                    if v2 != v and v2:
                        v2 += "…"
                    d.text((x0, y), v2, font=f_row, fill=C_BODY)
                    y += 44

            if skins:
                y += 26
                d.text((PAD, y), "🗡 本期皮肤", font=f_sec, fill=C_TITLE)
                y += 30 + 12
                for i, s in enumerate(skins, 1):
                    d.text((PAD, y), f"{i}. {s}", font=f_row, fill=C_BODY)
                    y += 40
                if skins_more:
                    d.text((PAD, y), f"… 还有 {skins_more} 款，详见下方全文", font=f_row, fill=C_META)
                    y += 40

            if sections:
                y += 26
                d.text((PAD, y), "📑 公告小节（官方原文结构）", font=f_sec, fill=C_TITLE)
                y += 30 + 12
                for i, s in enumerate(sections, 1):
                    d.text((PAD, y), f"{i}. {s}", font=f_row, fill=C_BODY)
                    y += 40
                if sections_more:
                    d.text((PAD, y), f"… 还有 {sections_more} 个小节，详见下方全文", font=f_row, fill=C_META)
                    y += 40

            y += 24
            d.line((PAD, y, W - PAD, y), fill=C_LINE, width=2)
            y += 2 + 18
            d.text((PAD, y), "👇 下一条为官方全文长图（原文直出，未经 AI 加工）", font=f_foot, fill=C_META)
            y += 26 + 10
            for ln in url_lines:
                d.text((PAD, y), ln, font=f_foot, fill=C_META)
                y += 32

            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            img.save(out_path, "PNG")
            return out_path
        except Exception as e:
            logger.warning(f"[wzry_watcher] 渲染速览卡失败: {e}")
            return None

    def _format_news(self, news_list: list, channel: str, keywords=None, max_items=8) -> str:
        kw = "，关键词：" + " ".join(keywords) if keywords else ""
        lines = [f"📢 王者荣耀·{channel}（最新 {len(news_list)} 条{kw}）"]
        for i, n in enumerate(news_list[:max_items], 1):
            t = n["time"][:16] if n["time"] else "时间未知"
            lines.append(f"{i}. {n['title']}（{t}）")
            lines.append(f"   {n['url']}")
        return "\n".join(lines)

    def _keywords(self) -> list:
        raw = self.config.get("keywords", "")
        if isinstance(raw, str):
            return [k.strip() for k in raw.split(",") if k.strip()] or DEFAULT_KEYWORDS
        return raw or DEFAULT_KEYWORDS

    # ==================== 后台推送 ====================

    def _ensure_task(self):
        """懒启动后台推送任务（确保 event loop 已运行后再创建）。"""
        if self._task is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is not None:
                self._task = loop.create_task(self._push_loop())

    async def _push_loop(self):
        while True:
            try:
                await asyncio.sleep(60)
                await self._check_and_push()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[wzry_watcher] 推送循环异常: {e}")

    async def _check_and_push(self):
        """遍历全部五个频道（热点/新闻/公告/活动/赛事），发现新条目即推送。"""
        if not self.config.get("push_enabled", False):
            return
        groups = [str(g) for g in (self.config.get("push_groups") or [])]
        if not groups:
            return
        seen = set(self.data.get("seen", []))
        all_fresh = []
        for chan_name, chanid in CHANNEL_MAP.items():
            try:
                news_list = await self._fetch_news(chanid, int(self.config.get("fetch_limit", 20)))
            except Exception as e:
                logger.warning(f"[wzry_watcher] 定时拉取 {chan_name} 失败: {e}")
                continue
            fresh = [n for n in news_list if n["id"] not in seen and self._match(n, self._keywords())]
            if fresh:
                all_fresh.append((chan_name, fresh))
        if not all_fresh:
            return
        # 记录已推送（seen 按 id 去重，跨频道也去重）
        for _, fresh in all_fresh:
            for n in fresh:
                seen.add(n["id"])
        self.data["seen"] = sorted(seen)[-SEEN_MAX:]
        self._save_data()
        # 推送：每个频道一条独立消息，附频道标签便于区分
        umo_map = self.data.get("umo_map", {})
        for gid, umo in umo_map.items():
            if gid not in groups:
                continue
            for chan_name, fresh in all_fresh:
                text = self._format_news(fresh, chan_name, self._keywords(), max_items=5)
                try:
                    await self.context.send_message(umo, MessageChain().message(text))
                    logger.info(f"[wzry_watcher] 已推送 {chan_name} {len(fresh)} 条到群 {gid}")
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"[wzry_watcher] 推送群 {gid} 失败: {e}")

    # ==================== 指令 ====================

    @filter.command("王者公告", alias={"王者动态", "王者更新", "王者活动"})
    async def cmd_query(self, event: AstrMessageEvent):
        """查询王者荣耀最新动态，可选关键词过滤。用法：王者公告 [频道] [关键词]"""
        self._ensure_task()
        logger.info(f"[wzry_watcher] 收到指令: {event.message_str!r} from {event.get_sender_id()}")
        await self._handle_query(event, from_push=False)

    @filter.command("王者公告原文", alias={"王者原文", "公告原文", "王者公告全文"})
    async def cmd_raw(self, event: AstrMessageEvent):
        """输出指定公告的原文：速览卡 + 全文长图（不经 AI 总结）。用法：王者公告原文 [序号|关键词]"""
        self._ensure_task()
        logger.info(f"[wzry_watcher] 收到原文指令: {event.message_str!r} from {event.get_sender_id()}")
        await self._handle_raw(event)

    @filter.command("王者订阅")
    async def cmd_subscribe(self, event: AstrMessageEvent):
        """管理订阅：王者订阅 / 王者订阅 on|off / 王者订阅 群 <群号>"""
        msg = (event.message_str or "").strip()
        parts = [p for p in msg.split() if p]
        cmd = parts[1].lower() if len(parts) > 1 else "status"
        if cmd in ("on", "开", "开启"):
            self.config["push_enabled"] = True
            await event.send(event.plain_result("✅ 已开启定时推送（当前间隔 {} 分钟，关键词：{}）。".format(
                self.config.get("push_interval_minutes", 60), " ".join(self._keywords()))))
            return
        if cmd in ("off", "关", "关闭"):
            self.config["push_enabled"] = False
            await event.send(event.plain_result("🛑 已关闭定时推送。"))
            return
        if cmd == "群" and len(parts) > 2:
            groups = [g for g in parts[2:] if g.isdigit()]
            if groups:
                self.config["push_groups"] = groups
                await event.send(event.plain_result(f"📢 推送目标群已设为：{' '.join(groups)}"))
                return
        # status
        groups = self.config.get("push_groups") or []
        umo_map = self.data.get("umo_map", {})
        status = "开" if self.config.get("push_enabled", False) else "关"
        await event.send(event.plain_result(
            "📡 王者荣耀动态订阅\n"
            f"状态：{status}\n"
            f"间隔：{self.config.get('push_interval_minutes', 60)} 分钟\n"
            f"关键词：{' '.join(self._keywords())}\n"
            f"推送群：{' '.join(map(str, groups)) if groups else '未设置'}\n"
            f"已记录群：{' '.join(umo_map.keys()) if umo_map else '无'}\n"
            f"指令：王者公告 [关键词]｜王者订阅 on/off｜王者订阅 群 <群号>"))

    async def _handle_raw(self, event: AstrMessageEvent):
        """输出指定公告的官方原文全文（官网正文直出，不经 AI 总结）。"""
        gid = str(event.get_group_id() or "")
        if gid:
            self._record_umo(gid, event.unified_msg_origin)
        msg = (event.message_str or "").strip()
        parts = [p for p in msg.split() if p][1:]
        chanid = CHANNEL_MAP.get(self.config.get("channel", "公告"), 1762)
        try:
            news_list = await self._fetch_news(chanid, int(self.config.get("fetch_limit", 20)))
        except Exception as e:
            logger.error(f"[wzry_watcher] 原文拉取失败: {e}")
            await event.send(event.plain_result("⚠️ 王者荣耀官网暂时无法访问，请稍后重试。"))
            return
        if not news_list:
            await event.send(event.plain_result("📭 当前没有可用的公告。"))
            return
        # 选择条目：序号优先，其次标题关键词
        picked = None
        if parts:
            if parts[0].isdigit():
                idx = int(parts[0]) - 1
                if 0 <= idx < len(news_list):
                    picked = news_list[idx]
            else:
                kw = " ".join(parts)
                for n in news_list:
                    if kw.lower() in (n["title"] + " " + n["tag"]).lower():
                        picked = n
                        break
        if picked is None:
            picked = news_list[0]

        # 视频类动态没有图文正文，直出链接
        if picked.get("is_video"):
            await event.send(event.plain_result("\n".join([
                f"📢 {picked['title']}",
                f"🕐 {picked['time'][:16] if picked['time'] else '时间未知'}",
                f"🔗 {picked['url']}",
                "（视频类动态，请打开官方链接观看）",
            ])))
            return

        # 图文公告：拉取正文全文并分片直出
        content = None
        try:
            content = await self._fetch_content(picked["id"])
        except Exception as e:
            logger.warning(f"[wzry_watcher] 正文拉取异常: {e}")
        if not content or not content.get("content"):
            logger.warning(f"[wzry_watcher] 正文不可用，回退条目模式 id={picked['id']}")
            await event.send(event.plain_result("\n".join([
                f"📢 {picked['title']}",
                f"🕐 {picked['time'][:16] if picked['time'] else '时间未知'}",
                f"🔗 {picked['url']}",
                "（正文暂时拉取不到，请打开官方链接查看）",
            ])))
            return

        meta_time = content["time"] or (picked["time"] or "")[:16]
        img_dir = os.path.dirname(self.data_path)
        ts = int(time.time() * 1000)
        # 字体预热：无中文字体的主机首次渲染时自动下载（线程内执行不阻塞）
        try:
            await asyncio.to_thread(ensure_fonts_downloaded)
        except Exception:
            pass
        # 速览卡（关键信息 + 小节目录，纯结构提取）
        card = None
        try:
            summary = self._extract_summary(content["content"])
            card = self._render_summary_image(
                content["title"] or picked["title"], meta_time, summary,
                picked["url"], os.path.join(img_dir, f"wzry_card_{ts}.png"),
            )
        except Exception as e:
            logger.warning(f"[wzry_watcher] 渲染速览卡异常: {e}")
        # 全文长图
        rendered = None
        try:
            rendered = self._render_raw_image(
                title=content["title"] or picked["title"],
                meta=meta_time,
                body=content["content"],
                footer=f"🔗 官方链接：{picked['url']}｜wzry_watcher 直出",
                out_path=os.path.join(img_dir, f"wzry_raw_{ts}.png"),
            )
        except Exception as e:
            logger.warning(f"[wzry_watcher] 渲染正文图片异常: {e}")

        if card:
            try:
                chain = MessageChain().file_image(card)
                # AstrBot 4.x 文本方法是 message()（plain() 是旧版 API，会 AttributeError）
                chain.message(f"\n🔗 {picked['url']}")
                await event.send(chain)
                await asyncio.sleep(0.6)
            except Exception as e:
                logger.warning(f"[wzry_watcher] 速览卡发送失败: {e}")
        if rendered:
            try:
                try:
                    chain = MessageChain().file_image(rendered)
                except TypeError:
                    chain = MessageChain().file_image(path=rendered)
                if not card:
                    chain.message(f"\n🔗 {picked['url']}")
                await event.send(chain)
                self._cleanup_old_images(img_dir)
                return
            except Exception as e:
                logger.warning(f"[wzry_watcher] 图片发送失败，回退文本分片: {e}")

        # 图片不可用时的文本分片回退
        header = "\n".join([
            f"📢 {content['title'] or picked['title']}",
            f"🕐 {meta_time}｜官网原文直出，未经 AI 加工",
            "─" * 18,
        ])
        footer_text = "─" * 18 + f"\n🔗 {picked['url']}"
        chunks = self._split_chunks(content["content"])
        total = len(chunks)
        for i, chunk in enumerate(chunks):
            piece = ""
            if i == 0:
                piece += header + "\n"
            piece += chunk
            if i == total - 1:
                piece += "\n" + footer_text
            elif total > 1:
                piece += f"\n（正文 {i + 1}/{total}，接下一条）"
            await event.send(event.plain_result(piece))
            if i < total - 1:
                await asyncio.sleep(0.8)

    def _cleanup_old_images(self, img_dir: str, max_age_seconds: int = 3600):
        """清理 1 小时前渲染的正文图片，防止磁盘堆积。"""
        try:
            now = time.time()
            for name in os.listdir(img_dir):
                if name.startswith(("wzry_raw_", "wzry_card_")) and name.endswith(".png"):
                    p = os.path.join(img_dir, name)
                    if now - os.path.getmtime(p) > max_age_seconds:
                        os.remove(p)
        except Exception as e:
            logger.warning(f"[wzry_watcher] 清理旧图失败: {e}")

    async def _handle_query(self, event: AstrMessageEvent, from_push: bool = False):
        # 记录群会话，供后续推送
        gid = str(event.get_group_id() or "")
        if gid:
            self._record_umo(gid, event.unified_msg_origin)
        # 解析参数：第一个参数若是频道名则切换频道，其余作为关键词
        msg = (event.message_str or "").strip()
        parts = [p for p in msg.split() if p][1:]
        channel = self.config.get("channel", "公告")
        keywords = []
        for p in parts:
            if p in CHANNEL_MAP:
                channel = p
            else:
                keywords.append(p)
        if not keywords:
            keywords = self._keywords()
        chanid = CHANNEL_MAP.get(channel, 1762)
        try:
            news_list = await self._fetch_news(chanid, int(self.config.get("fetch_limit", 20)))
        except Exception as e:
            logger.error(f"[wzry_watcher] 拉取失败: {e}")
            await event.send(event.plain_result("⚠️ 王者荣耀官网暂时无法访问，请稍后重试。"))
            return
        matched = [n for n in news_list if self._match(n, keywords)] if keywords else news_list
        if not matched:
            await event.send(event.plain_result(
                f"🔍 未发现匹配的条目（频道：{channel}，关键词：{' '.join(keywords)}）。"))
            return
        text = self._format_news(matched, channel, keywords, max_items=8)
        await event.send(event.plain_result(text))

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        """记录群会话标识，供定时推送使用。"""
        self._ensure_task()
        gid = str(event.get_group_id() or "")
        if gid:
            try:
                self._record_umo(gid, event.unified_msg_origin)
            except Exception as e:
                logger.warning(f"[wzry_watcher] 记录会话失败: {e}")

    async def terminate(self):
        """插件卸载时取消后台任务。"""
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
        self._task = None
        self._save_data()
