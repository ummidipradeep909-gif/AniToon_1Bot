from __future__ import annotations

import hashlib
import mimetypes
import html
import io
import os
import re
import struct
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

# Keep the total source-media read safely below 4 MB.
MAX_INITIAL_PROBE = 1 * 1024 * 1024
MAX_DEEP_PROBE = 2_560 * 1024
DEFAULT_CHUNK = 512 * 1024
ProgressFn = Callable[[str], Awaitable[None]]

LANG = {
    "eng":"English","en":"English","jpn":"Japanese","ja":"Japanese","hin":"Hindi","hi":"Hindi",
    "tel":"Telugu","te":"Telugu","tam":"Tamil","ta":"Tamil","mal":"Malayalam","ml":"Malayalam",
    "kan":"Kannada","kn":"Kannada","kor":"Korean","ko":"Korean","zho":"Chinese","chi":"Chinese","zh":"Chinese",
    "spa":"Spanish","es":"Spanish","fra":"French","fre":"French","fr":"French","deu":"German","ger":"German","de":"German",
    "ita":"Italian","it":"Italian","rus":"Russian","ru":"Russian","ara":"Arabic","ar":"Arabic","por":"Portuguese","pt":"Portuguese","und":"Undetermined",
}
CODEC = {
    "A_AAC":"AAC","A_AAC/MPEG2/LC":"AAC-LC","A_AAC/MPEG4/LC":"AAC-LC","A_AC3":"AC-3","A_EAC3":"E-AC-3","A_OPUS":"Opus","A_FLAC":"FLAC",
    "A_MPEG/L3":"MP3","A_VORBIS":"Vorbis","A_TRUEHD":"TrueHD","A_DTS":"DTS","A_DTS/LOSSLESS":"DTS-HD MA","V_MPEGH/ISO/HEVC":"H.265/HEVC",
    "V_MPEG4/ISO/AVC":"H.264/AVC","V_AV1":"AV1","V_VP9":"VP9","S_TEXT/UTF8":"SubRip/UTF-8","S_TEXT/ASS":"ASS","S_TEXT/SSA":"SSA",
    "S_TEXT/WEBVTT":"WebVTT","S_HDMV/PGS":"PGS","S_VOBSUB":"VobSub",
}
SUB_EXT={".srt":"SubRip (SRT)",".vtt":"WebVTT",".ass":"ASS",".ssa":"SSA",".sub":"SUB",".ttml":"TTML",".smi":"SAMI",".sami":"SAMI",".sbv":"SBV"}
AUDIO_EXT={".mp3":"MP3",".flac":"FLAC",".wav":"WAV",".wave":"WAV",".ogg":"Ogg",".oga":"Ogg",".opus":"Opus",".m4a":"M4A",".aac":"AAC",".ac3":"AC-3",".eac3":"E-AC-3",".mka":"Matroska audio"}

def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try: value=int(os.getenv(name,""))
    except ValueError: return default
    return max(lo,min(hi,value))

def initial_probe_bytes()->int: return _env_int("FILE_PROBE_BYTES",MAX_INITIAL_PROBE,512*1024,MAX_INITIAL_PROBE)
def deep_probe_budget()->int: return _env_int("FILE_DEEP_PROBE_BYTES",2_560*1024,2*1024*1024,MAX_DEEP_PROBE)
def probe_chunk()->int: return _env_int("FILE_PROBE_CHUNK_BYTES",DEFAULT_CHUNK,128*1024,DEFAULT_CHUNK)

@dataclass(slots=True)
class ProbePiece:
    offset:int
    data:bytes
    label:str

@dataclass(slots=True)
class Report:
    filename:str="Telegram media file"; size:int|None=None; mime:str|None=None; ext:str=""; detected:str="Detected media"; media_kind:str="File"
    sampled:int=0; sample_hash:str=""; notes:list[str]=field(default_factory=list)
    video:dict[str,Any]=field(default_factory=dict); audio:dict[str,Any]=field(default_factory=dict)
    subtitles:list[dict[str,Any]]=field(default_factory=list); container:dict[str,str]=field(default_factory=dict)
    probe_ranges:list[str]=field(default_factory=list)
    previews:list[dict[str,Any]]=field(default_factory=list)

def human(n:int|None)->str:
    if n is None:return "Not available"
    x=float(n)
    for unit in ("B","KiB","MiB","GiB","TiB"):
        if x<1024 or unit=="TiB": return f"{int(x)} B" if unit=="B" else f"{x:.2f} {unit}"
        x/=1024
    return f"{n} B"

def _ext(name:str)->str:
    p=(name or "").rfind("."); return name[p:].lower() if p>0 else ""

def magic(data:bytes,name:str,mime:str|None)->tuple[str,str]:
    h=data[:64]
    ext=_ext(name)
    mime_clean=(mime or "").strip().lower()

    if h.startswith(b"\x1aE\xdf\xa3"): return "Matroska/WebM container","mkv"
    if len(data)>=12 and data[4:8]==b"ftyp": return "ISO Base Media File Format (MP4/MOV)","mp4"
    if h.startswith(b"fLaC"): return "FLAC audio","flac"
    if h.startswith(b"OggS"): return "Ogg container","ogg"
    if h.startswith(b"ID3"): return "MP3 audio","mp3"
    if h.startswith(b"RIFF") and len(h)>=12 and h[8:12]==b"WAVE": return "WAV audio","wav"
    if h.startswith(b"%PDF-"): return "PDF document","pdf"
    if h.startswith(b"PK\x03\x04"): return "ZIP archive","zip"
    if h.startswith(b"\x89PNG\r\n\x1a\n"): return "PNG image","png"
    if h.startswith(b"\xff\xd8\xff"): return "JPEG image","jpeg"

    txt=data[:8192].decode("utf-8","replace").lstrip("\ufeff \t\r\n")
    if txt.startswith("WEBVTT"): return "WebVTT subtitle","vtt"
    if "[Events]" in txt and re.search(r"^\s*\[Script Info\]",txt,re.I|re.M): return "ASS/SSA subtitle","ass"
    if re.search(r"<tt(?:\s|>)",txt,re.I): return "TTML subtitle","ttml"
    if re.search(r"\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*",txt): return "SubRip subtitle","srt"

    ext_labels={
        ".mkv":("Matroska container","mkv"), ".webm":("WebM container","mkv"),
        ".mp4":("MP4 media","mp4"), ".m4v":("MPEG-4 video","mp4"), ".mov":("QuickTime media","mp4"),
        ".m2ts":("MPEG Transport Stream","ts"), ".ts":("MPEG Transport Stream","ts"),
        ".avi":("AVI video","avi"), ".wmv":("Windows Media Video","wmv"), ".flv":("Flash Video","flv"),
        ".mpg":("MPEG video","mpeg"), ".mpeg":("MPEG video","mpeg"), ".3gp":("3GPP media","3gp"),
        ".flac":("FLAC audio","flac"), ".mp3":("MP3 audio","mp3"), ".wav":("WAV audio","wav"),
        ".m4a":("MPEG-4 audio","m4a"), ".aac":("AAC audio","aac"), ".opus":("Opus audio","opus"),
        ".ogg":("Ogg audio","ogg"), ".oga":("Ogg audio","ogg"), ".mka":("Matroska audio","mka"),
        ".srt":("SubRip subtitle","srt"), ".ass":("ASS/SSA subtitle","ass"), ".ssa":("ASS/SSA subtitle","ssa"),
        ".vtt":("WebVTT subtitle","vtt"), ".ttml":("TTML subtitle","ttml"),
    }
    if ext in ext_labels:return ext_labels[ext]

    if mime_clean:
        for prefix,label in (
            ("video/","Video media"),("audio/","Audio media"),
            ("text/","Text document"),("image/","Image file"),
        ):
            if mime_clean.startswith(prefix):
                return f"{label} ({mime_clean})","binary"
        if mime_clean=="application/pdf": return "PDF document","pdf"
        if mime_clean=="application/zip": return "ZIP archive","zip"
        return mime_clean,"binary"

    guessed,_=mimetypes.guess_type(name)
    if guessed:return guessed,"binary"
    return "Binary file","binary"

def _vint(data:bytes,pos:int):
    if pos>=len(data):return None
    first=data[pos]; mask=0x80; width=1
    while width<=8 and not(first&mask):mask>>=1;width+=1
    if width>8 or pos+width>len(data):return None
    value=first&(mask-1)
    for i in range(1,width):value=(value<<8)|data[pos+i]
    return value,width

def _elem(data:bytes,pos:int):
    if pos>=len(data):return None
    first=data[pos]; mask=0x80; il=1
    while il<=4 and not(first&mask):mask>>=1;il+=1
    if il>4 or pos+il>=len(data):return None
    eid=int.from_bytes(data[pos:pos+il],"big")
    si=_vint(data,pos+il)
    if not si:return None
    size,sl=si
    start=pos+il+sl
    unknown=size==(1<<(7*sl))-1
    declared_end=len(data) if unknown else start+size

    # Partial byte-range scans routinely end inside a large EBML element
    # such as Segment or Tracks. Treat the visible part as a valid truncated
    # element so its children can still be parsed.
    end=min(len(data),declared_end)
    if end<start:return None
    return eid,start,end

def _children(data:bytes,start:int,end:int):
    p=start
    while p<end:
        x=_elem(data,p)
        if not x:break
        yield x
        if x[2]<=p:break
        p=x[2]

def _text(data:bytes,s:int,e:int)->str:return data[s:e].decode("utf-8","replace").strip("\x00 \t\r\n")
def _flt(data:bytes,s:int,e:int):
    try:return struct.unpack(">f" if e-s==4 else ">d",data[s:e])[0] if e-s in (4,8) else None
    except struct.error:return None
def _lang(code:str|None)->str|None:
    c=(code or "").lower().replace("_","-")
    if not c or c in {"und", "unknown", "unk"}:
        return None
    return LANG.get(c) or LANG.get(c.split("-")[0])
def _fmtsec(sec:float|None)->str:
    if sec is None or sec<0:return "Not available"
    total=int(round(sec)); h,rem=divmod(total,3600); m,s=divmod(rem,60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def _segment_start(data:bytes):
    p=data.find(b"\x18\x53\x80\x67")
    if p<0:return None
    x=_elem(data,p); return (p,x[1]) if x else None

def _seek_targets(data:bytes)->dict[int,int]:
    seg=_segment_start(data)
    if not seg:return {}
    _,seg_start=seg; p=data.find(b"\x11\x4D\x9B\x74",seg_start)
    if p<0:return {}
    x=_elem(data,p)
    if not x:return {}
    targets={}
    for eid,s,e in _children(data,x[1],x[2]):
        if eid!=0x4DBB:continue
        tid=offset=None
        for cid,cs,ce in _children(data,s,e):
            b=data[cs:ce]
            if cid==0x53AB and b:tid=int.from_bytes(b,"big")
            elif cid==0x53AC and b:offset=int.from_bytes(b,"big")
        if tid is not None and offset is not None:targets[tid]=seg_start+offset
    return targets

def _track(data:bytes,s:int,e:int)->dict[str,Any]|None:
    typ=num=name=lang=lang_i=cid=cname=uid=None
    default=forced=enabled=sdh=vi=original=commentary=None
    channels=depth=width=height=None
    display_width=display_height=None
    crop_left=crop_right=crop_top=crop_bottom=None
    interlaced=stereo_mode=alpha_mode=None
    default_duration=codec_delay=seek_preroll=None
    rate=output_rate=None

    for eid,cs,ce in _children(data,s,e):
        b=data[cs:ce]
        if eid==0xD7 and b:num=int.from_bytes(b,"big")
        elif eid==0x73C5 and b:uid=int.from_bytes(b,"big")
        elif eid==0x83 and b:typ={1:"video",2:"audio",17:"subtitles"}.get(int.from_bytes(b,"big"),"other")
        elif eid==0x536E:name=_text(data,cs,ce)
        elif eid==0x22B59C:lang=_text(data,cs,ce)
        elif eid==0x22B59D:lang_i=_text(data,cs,ce)
        elif eid==0x86:cid=_text(data,cs,ce)
        elif eid==0x258688:cname=_text(data,cs,ce)
        elif eid==0x88 and b:default=bool(int.from_bytes(b,"big"))
        elif eid==0x55AA and b:forced=bool(int.from_bytes(b,"big"))
        elif eid==0xB9 and b:enabled=bool(int.from_bytes(b,"big"))
        elif eid==0x55AB and b:sdh=bool(int.from_bytes(b,"big"))
        elif eid==0x55AC and b:vi=bool(int.from_bytes(b,"big"))
        elif eid==0x55AE and b:original=bool(int.from_bytes(b,"big"))
        elif eid==0x55AF and b:commentary=bool(int.from_bytes(b,"big"))
        elif eid==0x9F and b:channels=int.from_bytes(b,"big")
        elif eid==0xB5:rate=_flt(data,cs,ce)
        elif eid==0x78B5:output_rate=_flt(data,cs,ce)
        elif eid==0x6264 and b:depth=int.from_bytes(b,"big")
        elif eid==0x23E383 and b:default_duration=int.from_bytes(b,"big")
        elif eid==0x56AA and b:codec_delay=int.from_bytes(b,"big")
        elif eid==0x56BB and b:seek_preroll=int.from_bytes(b,"big")
        elif eid==0xE1:
            for nid,ns,ne in _children(data,cs,ce):
                nb=data[ns:ne]
                if nid==0x9F and nb:channels=int.from_bytes(nb,"big")
                elif nid==0xB5:rate=_flt(data,ns,ne)
                elif nid==0x78B5:output_rate=_flt(data,ns,ne)
                elif nid==0x6264 and nb:depth=int.from_bytes(nb,"big")
        elif eid==0xE0:
            for nid,ns,ne in _children(data,cs,ce):
                nb=data[ns:ne]
                if nid==0xB0 and nb:width=int.from_bytes(nb,"big")
                elif nid==0xBA and nb:height=int.from_bytes(nb,"big")
                elif nid==0x54B0 and nb:display_width=int.from_bytes(nb,"big")
                elif nid==0x54BA and nb:display_height=int.from_bytes(nb,"big")
                elif nid==0x54AA and nb:crop_left=int.from_bytes(nb,"big")
                elif nid==0x54BB and nb:crop_right=int.from_bytes(nb,"big")
                elif nid==0x54CC and nb:crop_top=int.from_bytes(nb,"big")
                elif nid==0x54DD and nb:crop_bottom=int.from_bytes(nb,"big")
                elif nid==0x9A and nb:interlaced=bool(int.from_bytes(nb,"big"))
                elif nid==0x53B8 and nb:stereo_mode=int.from_bytes(nb,"big")
                elif nid==0x53C0 and nb:alpha_mode=int.from_bytes(nb,"big")

    if typ not in {"audio","video","subtitles"}:return None
    use_lang=lang_i or lang
    lname=_lang(use_lang)
    codec_display=cname or CODEC.get(cid or "") or cid
    if codec_display and str(codec_display).strip().lower() in {"unknown","unk","undefined","und"}:
        codec_display=None

    base_type={"audio":"Audio","video":"Video","subtitles":"Subtitle"}[typ]
    clean_name=name.strip() if name else None
    if clean_name and clean_name.lower() in {"unknown","und","undefined","audio","video","subtitle","track"}:
        clean_name=None
    track_label=f" {num}" if num is not None else ""

    if clean_name:
        display_name=clean_name; name_source="track metadata"
    elif lname and codec_display:
        display_name=f"{lname} {codec_display} {base_type} Track{track_label}"; name_source="language + codec metadata"
    elif lname:
        display_name=f"{lname} {base_type} Track{track_label}"; name_source="language metadata"
    elif codec_display:
        display_name=f"{codec_display} {base_type} Track{track_label}"; name_source="codec metadata"
    else:
        display_name=f"{base_type} Track{track_label}"; name_source="stream type fallback"

    d={"type":typ,"track":str(num) if num is not None else None,
       "name":display_name,"display_name":display_name,"name_source":name_source,
       "track_uid":str(uid) if uid is not None else None,
       "language":use_lang,"language_name":lname,"codec":cid,"codec_name":codec_display}
    for k,v in (("default",default),("enabled",enabled),("forced",forced if typ=="subtitles" else None),
                ("hearing_impaired",sdh if typ=="subtitles" else None),
                ("visual_impaired",vi if typ=="subtitles" else None),
                ("original",original),("commentary",commentary)):
        if v is not None:d[k]="yes" if v else "no"

    if typ=="audio":
        if channels is not None:d["channels"]=str(channels)
        if rate and rate>0:d["sample_rate"]=f"{rate/1000:.1f} kHz"
        if output_rate and output_rate>0 and (not rate or abs(output_rate-rate)>0.001):
            d["output_sample_rate"]=f"{output_rate/1000:.1f} kHz"
        if depth is not None:d["bit_depth"]=f"{depth} bit"
        if codec_delay is not None:d["codec_delay"]=f"{codec_delay} ns"
        if seek_preroll is not None:d["seek_preroll"]=f"{seek_preroll} ns"

    if typ=="video":
        if width and height:d["dimensions"]=f"{width} × {height}"
        if display_width and display_height:d["display_dimensions"]=f"{display_width} × {display_height}"
        if any(v is not None for v in (crop_left,crop_right,crop_top,crop_bottom)):
            d["crop"]=(f"L{crop_left or 0} / R{crop_right or 0} / "
                       f"T{crop_top or 0} / B{crop_bottom or 0}")
        if interlaced is not None:d["scan_type"]="Interlaced" if interlaced else "Progressive"
        if stereo_mode is not None:
            d["stereo_mode"]={0:"Mono",1:"Side by Side (Left first)",2:"Side by Side (Right first)",
                              3:"Top Bottom (Right first)",4:"Top Bottom (Left first)",
                              5:"Checkerboard (Right first)",6:"Checkerboard (Left first)",
                              7:"Row interleaved (Right first)",8:"Row interleaved (Left first)",
                              9:"Column interleaved (Right first)",10:"Column interleaved (Left first)",
                              11:"Anaglyph (cyan/red)",12:"Side by Side (left to right, half)",
                              13:"Side by Side (right to left, half)",14:"Top Bottom (half)"
                             }.get(stereo_mode,str(stereo_mode))
        if alpha_mode is not None:d["alpha_mode"]="Present" if alpha_mode else "None"
        if default_duration and default_duration>0:d["frame_rate"]=f"{1_000_000_000/default_duration:.3f} fps"

    return {k:v for k,v in d.items() if v not in (None,"")}

def _merge(report:Report,track:dict[str,Any]):
    bucket=report.audio.setdefault("tracks",[]) if track["type"]=="audio" else report.video.setdefault("tracks",[]) if track["type"]=="video" else report.subtitles
    key=(track.get("type"),track.get("track"),track.get("language"),track.get("name"),track.get("codec"))
    if not any((x.get("type"),x.get("track"),x.get("language"),x.get("name"),x.get("codec"))==key for x in bucket):bucket.append(track)

def _tracks(data:bytes,report:Report):
    p=data.find(b"\x16\x54\xAE\x6B"); total=0
    while p>=0:
        x=_elem(data,p)
        if not x:break
        for eid,s,e in _children(data,x[1],x[2]):
            if eid==0xAE:
                t=_track(data,s,e)
                if t:_merge(report,t);total+=1
        p=data.find(b"\x16\x54\xAE\x6B",x[2])
    return total

def _info(data:bytes,report:Report):
    p=data.find(b"\x15\x49\xA9\x66")
    while p>=0:
        x=_elem(data,p)
        if not x:break
        scale=1_000_000; dur=title=muxing_app=writing_app=date_utc=None
        for eid,s,e in _children(data,x[1],x[2]):
            b=data[s:e]
            if eid==0x2AD7B1 and b:scale=int.from_bytes(b,"big")
            elif eid==0x4489:dur=_flt(data,s,e)
            elif eid==0x7BA9:title=_text(data,s,e)
            elif eid==0x4D80:muxing_app=_text(data,s,e)
            elif eid==0x5741:writing_app=_text(data,s,e)
            elif eid==0x4461 and b and len(b) in (8,16):
                with suppress(Exception):
                    date_utc=int.from_bytes(b[:8],"big")
        if dur is not None and dur>=0:
            sec=dur*scale/1_000_000_000
            report.container["runtime"]=_fmtsec(sec)
            report.container["runtime_seconds"]=f"{sec:.3f}"
        if title:report.container["title"]=title
        if muxing_app:report.container["muxing_app"]=muxing_app
        if writing_app:report.container["writing_app"]=writing_app
        if date_utc is not None:
            with suppress(Exception):
                report.container["date_utc"]=(datetime(2001,1,1,tzinfo=timezone.utc)+timedelta(microseconds=date_utc)).isoformat()
        p=data.find(b"\x15\x49\xA9\x66",x[2])


MP4_CODEC = {
    b"avc1":"H.264/AVC", b"avc3":"H.264/AVC",
    b"hev1":"H.265/HEVC", b"hvc1":"H.265/HEVC",
    b"av01":"AV1", b"vp09":"VP9", b"vp08":"VP8",
    b"mp4a":"AAC", b"ac-3":"AC-3", b"ec-3":"E-AC-3",
    b"Opus":"Opus", b"fLaC":"FLAC", b"alaw":"G.711 A-law",
    b"ulaw":"G.711 μ-law", b"tx3g":"Timed Text", b"wvtt":"WebVTT",
    b"stpp":"TTML", b"c608":"CEA-608", b"c708":"CEA-708",
}

MP4_CONTAINER_BOXES = {
    b"moov", b"trak", b"mdia", b"minf", b"stbl", b"edts", b"dinf",
    b"udta", b"meta", b"ilst", b"wave", b"ipro", b"sinf", b"schi",
}

def _mp4_boxes(data:bytes,start:int=0,end:int|None=None):
    end=len(data) if end is None else min(len(data),end)
    p=max(0,start)
    while p+8<=end:
        size=int.from_bytes(data[p:p+4],"big")
        typ=data[p+4:p+8]
        header=8
        if size==1:
            if p+16>end:break
            size=int.from_bytes(data[p+8:p+16],"big")
            header=16
        elif size==0:
            size=end-p
        if size<header or p+size>end:break
        payload_start=p+header
        payload_end=p+size
        yield typ,payload_start,payload_end
        p=payload_end

def _mp4_children(data:bytes,s:int,e:int):
    yield from _mp4_boxes(data,s,e)

def _mp4_fullbox_version(data:bytes,s:int,e:int)->int|None:
    return data[s] if s<e else None

def _mp4_handler(data:bytes,s:int,e:int)->tuple[str|None,str|None]:
    version=_mp4_fullbox_version(data,s,e)
    if version is None or s+12>e:return None,None
    handler=data[s+8:s+12]
    name=data[s+24:e].split(b"\x00",1)[0].decode("utf-8","replace").strip() if s+24<e else ""
    return handler.decode("latin1","replace"), name or None

def _mp4_language(data:bytes,s:int,e:int)->str|None:
    if s>=e:return None
    version=data[s]
    lang_pos=s+12 if version==0 else s+20
    if lang_pos+2>e:return None
    packed=int.from_bytes(data[lang_pos:lang_pos+2],"big")
    chars=[]
    for shift in (10,5,0):
        chars.append(chr(((packed>>shift)&0x1F)+0x60))
    code="".join(chars)
    return code if code != "@@@" else None

def _mp4_track_id(data:bytes,s:int,e:int)->int|None:
    if s+12>e:return None
    version=data[s]
    pos=s+12 if version==0 else s+20
    if pos+4>e:return None
    return int.from_bytes(data[pos:pos+4],"big")

def _mp4_stsd_entries(data:bytes,s:int,e:int):
    if s+8>e:return
    count=int.from_bytes(data[s+4:s+8],"big")
    p=s+8
    for _ in range(min(count,64)):
        if p+8>e:break
        size=int.from_bytes(data[p:p+4],"big")
        typ=data[p+4:p+8]
        if size<8 or p+size>e:break
        yield typ,p+8,p+size
        p+=size

def _mp4_track_name(handler_name:str|None,handler_type:str,codec_display:str|None,track_id:int|None)->str:
    clean=(handler_name or "").strip()
    if clean and clean.lower() not in {"video","audio","sound","subtitle","subtitles","text","data","handler"}:
        return clean
    base={"vide":"Video","soun":"Audio","subt":"Subtitle","text":"Subtitle","sbtl":"Subtitle","clcp":"Subtitle"}.get(handler_type, "Media")
    label=f" {track_id}" if track_id is not None else ""
    if codec_display:
        return f"{codec_display} {base} Track{label}"
    return f"{base} Track{label}"

def _mp4_tracks(data:bytes,report:Report)->int:
    total=0
    for typ,moov_s,moov_e in _mp4_boxes(data):
        if typ!=b"moov":continue
        for ttyp,trak_s,trak_e in _mp4_children(data,moov_s,moov_e):
            if ttyp!=b"trak":continue
            track_id=None; handler_type=None; handler_name=None; language=None; codec=None
            dimensions=None; channels=None; sample_rate=None; bit_depth=None
            duration=None; timescale=None
            subtitle_format=None

            tkhd_box=None; mdia_box=None
            for ctyp,cs,ce in _mp4_children(data,trak_s,trak_e):
                if ctyp==b"tkhd":tkhd_box=(cs,ce)
                elif ctyp==b"mdia":mdia_box=(cs,ce)
                elif ctyp==b"udta":
                    for utyp,us,ue in _mp4_children(data,cs,ce):
                        if utyp in {b"name",b"\xA9nam"} and ue>us:
                            text=data[us:ue].decode("utf-8","replace").strip("\x00 \t\r\n")
                            if text:handler_name=text

            if tkhd_box:
                track_id=_mp4_track_id(data,tkhd_box[0],tkhd_box[1])

            if not mdia_box:continue
            md_s,md_e=mdia_box
            stsd_entries=[]
            for mtyp,ms,me in _mp4_children(data,md_s,md_e):
                if mtyp==b"hdlr":
                    handler_type,maybe_name=_mp4_handler(data,ms,me)
                    handler_name=handler_name or maybe_name
                elif mtyp==b"mdhd":
                    language=_mp4_language(data,ms,me)
                    version=data[ms] if ms<me else 0
                    base=ms+4
                    if version==0 and base+12<=me:
                        timescale=int.from_bytes(data[base+4:base+8],"big")
                        duration=int.from_bytes(data[base+8:base+12],"big")
                    elif version==1 and base+28<=me:
                        timescale=int.from_bytes(data[base+12:base+16],"big")
                        duration=int.from_bytes(data[base+16:base+24],"big")
                elif mtyp==b"minf":
                    for ntyp,ns,ne in _mp4_children(data,ms,me):
                        if ntyp!=b"stbl":continue
                        for sttyp,ss,se in _mp4_children(data,ns,ne):
                            if sttyp==b"stsd":
                                stsd_entries=list(_mp4_stsd_entries(data,ss,se) or [])

            if not stsd_entries:continue
            # Usually one primary sample entry per track. Keep all distinct entries
            # only when they represent different codecs, which can occur in text/media tracks.
            seen_codecs=set()
            for sample_type,ss,se in stsd_entries:
                if sample_type in seen_codecs:continue
                seen_codecs.add(sample_type)
                codec_display=MP4_CODEC.get(sample_type, sample_type.decode("latin1","replace").strip() or None)
                is_video=handler_type=="vide" or sample_type in {b"avc1",b"avc3",b"hev1",b"hvc1",b"av01",b"vp09",b"vp08"}
                is_audio=handler_type=="soun" or sample_type in {b"mp4a",b"ac-3",b"ec-3",b"Opus",b"fLaC"}
                is_sub=handler_type in {"subt","text","sbtl","clcp"} or sample_type in {b"tx3g",b"wvtt",b"stpp",b"c608",b"c708"}

                item={
                    "type":"video" if is_video else "audio" if is_audio else "subtitles" if is_sub else None,
                    "track":str(track_id) if track_id is not None else None,
                    "language":language,
                    "language_name":_lang(language),
                    "codec":sample_type.decode("latin1","replace"),
                    "codec_name":codec_display,
                    "name":_mp4_track_name(handler_name,handler_type or "",codec_display,track_id),
                    "display_name":_mp4_track_name(handler_name,handler_type or "",codec_display,track_id),
                    "name_source":"MP4 handler/track metadata",
                }
                if duration is not None and timescale:
                    item["duration"]=_fmtsec(duration/timescale)

                if is_video and se-ss>=28:
                    width=int.from_bytes(data[ss+24:ss+26],"big")
                    height=int.from_bytes(data[ss+26:ss+28],"big")
                    if width and height:item["dimensions"]=f"{width} × {height}"

                if is_audio and se-ss>=20:
                    channels=int.from_bytes(data[ss+16:ss+18],"big")
                    depth=int.from_bytes(data[ss+18:ss+20],"big")
                    if channels:item["channels"]=str(channels)
                    if depth:item["bit_depth"]=f"{depth} bit"
                    if se-ss>=28:
                        rate_fixed=int.from_bytes(data[ss+24:ss+28],"big")
                        rate=(rate_fixed>>16)+(rate_fixed & 0xFFFF)/65536
                        if rate>0:item["sample_rate"]=f"{rate/1000:.3f} kHz"

                if is_sub:
                    subtitle_format=codec_display
                    if subtitle_format:item["subtitle_format"]=subtitle_format

                if item["type"]:
                    _merge(report,item)
                    total+=1
            # A track with a non-standard handler still gets represented as generic media.
    return total

def _generic(data:bytes,kind:str,report:Report):
    if kind=="flac" and len(data)>=42:
        p=4
        while p+4<=len(data):
            last=bool(data[p]&0x80);typ=data[p]&0x7f;n=int.from_bytes(data[p+1:p+4],"big");s,e=p+4,p+4+n
            if e>len(data):break
            if typ==0 and n>=34:
                packed=int.from_bytes(data[s+10:s+18],"big");report.audio.update(sample_rate=f"{packed>>44} Hz",channels=str(((packed>>41)&7)+1),bits_per_sample=str(((packed>>36)&31)+1));break
            if last:break
            p=e
    elif kind=="wav" and len(data)>=20:
        p=12
        while p+8<=len(data):
            cid=data[p:p+4];n=int.from_bytes(data[p+4:p+8],"little");s,e=p+8,p+8+n
            if e>len(data):break
            if cid==b"fmt " and n>=16:report.audio.update(channels=str(int.from_bytes(data[s+2:s+4],"little")),sample_rate=f"{int.from_bytes(data[s+4:s+8],'little')} Hz",bits_per_sample=str(int.from_bytes(data[s+14:s+16],'little')));break
            p=e+(n&1)
    elif kind=="ogg":
        p=data.find(b"OpusHead")
        if p>=0 and p+16<=len(data):report.audio.update(codec="Opus",channels=str(data[p+9]),sample_rate=f"{int.from_bytes(data[p+12:p+16],'little')} Hz")
        else:
            p=data.find(b"vorbis")
            if p>=0 and p+16<=len(data):report.audio.update(codec="Vorbis",channels=str(data[p+11]),sample_rate=f"{int.from_bytes(data[p+12:p+16],'little')} Hz")
    elif kind=="mp4":
        parsed=_mp4_tracks(data,report)
        handlers=[]
        for m in re.finditer(b"hdlr",data[:2*1024*1024]):
            p=m.start()
            if p+12<=len(data):
                h=data[p+8:p+12].decode("latin1","replace")
                if h in {"soun","vide","subt","text","clcp","sbtl"} and h not in handlers:
                    handlers.append(h)
        if handlers:report.container["handlers_in_sample"]=", ".join(handlers)
        if not parsed:
            ac=[x for x in (b"mp4a",b"ac-3",b"ec-3",b"Opus") if x in data]
            if ac:report.audio["sample_codecs"]=", ".join(x.decode("latin1") for x in dict.fromkeys(ac))
            sc=[x for x in (b"tx3g",b"wvtt",b"stpp",b"c608",b"c708") if x in data]
            if sc:
                report.subtitles.append({
                    "name":"Embedded MP4 subtitle/text",
                    "format":", ".join(x.decode("latin1") for x in dict.fromkeys(sc)),
                    "source":"sample entry",
                })

def _probe_ranges(total:int|None,budget:int,initial:int,targets:dict[int,int]):
    ranges=[];used=initial
    # TrackEntry metadata is the highest-value part of the report. Read it
    # first, then use whatever remains for Matroska Info/runtime/title data.
    for eid,label,lim in (
        (0x1654AE6B,"Matroska Tracks",16*1024*1024),
        (0x1549A966,"Matroska Info",512*1024),
    ):
        if used>=budget:break
        off=targets.get(eid)
        if off is None or off<initial:continue
        n=min(lim,budget-used)
        if total is not None:n=min(n,max(1,total-off))
        if n>0:ranges.append((off,n,label));used+=n
    return ranges

def _adaptive_ranges(total:int|None,budget:int,used:int,initial:int):
    if not total or used>=budget:return
    preferred_mib=[1,2,4,8,12,16,24,32,48,64,80,96,112,128,160,192,256,384,512]
    window=256*1024
    seen=set()
    for mib in preferred_mib:
        off=mib*1024*1024
        if off>=total:continue
        off=max(initial,off)
        if off in seen:continue
        seen.add(off)
        n=min(window,budget-used,total-off)
        if n<=0:break
        yield off,n,f"metadata window {mib} MiB"
        used+=n
        if used>=budget:return
    for label,off in (
        ("tail metadata window",max(initial,total-window)),
        ("near-tail metadata window",max(initial,total-2*window)),
    ):
        if off in seen or used>=budget or off>=total:continue
        n=min(window,budget-used,total-off)
        if n>0:
            seen.add(off);yield off,n,label;used+=n
            if used>=budget:return

async def _read_range(client,media,total,offset,n):
    chunk=min(probe_chunk(),n);out=io.BytesIO()
    try:
        async for part in client.iter_download(media,offset=max(0,offset),limit=(n+chunk-1)//chunk,chunk_size=chunk,request_size=chunk,file_size=total):
            remain=n-out.tell()
            if remain<=0:break
            out.write(bytes(part[:remain]))
            if out.tell()>=n:break
        return out.getvalue(),None
    except Exception as e:return out.getvalue(),f"{type(e).__name__}: {e}"

def _report(message:Any,pieces:list[ProbePiece])->Report:
    f=getattr(message,"file",None);name=str(getattr(f,"name",None) or "telegram_file");size=getattr(f,"size",None);mime=getattr(f,"mime_type",None)
    first=next((x for x in pieces if x.offset==0),pieces[0]);detected,kind=magic(first.data,name,mime)
    r=Report(filename=name,size=int(size) if isinstance(size,int) else None,mime=str(mime) if mime else None,ext=_ext(name),detected=detected,media_kind="Video" if (mime or "").startswith("video/") else "File",sampled=sum(len(x.data) for x in pieces),sample_hash=hashlib.sha256(b"".join(x.data for x in pieces)).hexdigest())
    if getattr(f,"duration",None) is not None:
        r.container["runtime"]=_fmtsec(float(f.duration));r.container["runtime_source"]="Telegram media metadata"
    if getattr(f,"width",None) and getattr(f,"height",None):
        r.video["dimensions"]=f"{int(f.width)} × {int(f.height)}"
        if kind=="mp4" and not r.video.get("tracks"):
            r.video["tracks"]=[{
                "type":"video",
                "track":"1",
                "name":"Video Track 1",
                "display_name":"Video Track 1",
                "name_source":"Telegram media attributes",
                "dimensions":r.video["dimensions"],
            }]
    for p in pieces:
        if kind=="mkv":_info(p.data,r);_tracks(p.data,r)
        else:_generic(p.data,kind,r)
    if kind=="mkv":
        if not r.audio.get("tracks"):r.notes.append("No audio TrackEntry found in the probed metadata windows.")
        if not r.subtitles:r.notes.append("No subtitle TrackEntry found in the probed metadata windows.")
        if not r.audio.get("tracks"):_codec_hints(b"".join(x.data for x in pieces),r)
    if r.container.get("runtime") and r.size:
        try:
            sec=float(r.container["runtime_seconds"]) if r.container.get("runtime_seconds") else None
            if sec and sec>0:r.container["average_bitrate"]=f"{(r.size*8/sec)/1_000_000:.2f} Mbps"
        except ValueError:pass
    for x in pieces:r.probe_ranges.append(f"{x.label}: +{human(x.offset)} ({human(len(x.data))})")
    if r.ext in SUB_EXT and not r.subtitles:r.subtitles.append({"name":SUB_EXT[r.ext],"format":SUB_EXT[r.ext],"source":"filename extension"})
    if r.ext in AUDIO_EXT and not r.audio:r.audio["format_hint"]=AUDIO_EXT[r.ext]
    r.notes.append("Partial scan only: the scanner intentionally stopped before the end of the file.")
    if r.size and r.sampled<r.size:r.notes.append("Metadata outside the probed ranges can remain undetected.")
    return r

def _codec_hints(data:bytes,r:Report):
    for marker,name in ((b"A_AAC","AAC"),(b"A_AC3","AC-3"),(b"A_EAC3","E-AC-3"),(b"A_OPUS","Opus"),(b"A_FLAC","FLAC"),(b"A_MPEG/L3","MP3"),(b"A_VORBIS","Vorbis")):
        if marker in data:
            r.audio.setdefault("sample_codecs",name)
            if not isinstance(r.audio.get("tracks"), list):
                r.audio["tracks"] = []
            if not r.audio["tracks"]:
                r.audio["tracks"].append({
                    "type": "audio",
                    "track": "1",
                    "name": name,
                    "display_name": name,
                    "name_source": "codec marker fallback",
                    "codec_name": name,
                })
    for marker,name in ((b"S_TEXT/UTF8","SubRip/UTF-8"),(b"S_TEXT/ASS","ASS"),(b"S_TEXT/SSA","SSA"),(b"S_TEXT/WEBVTT","WebVTT"),(b"S_HDMV/PGS","PGS"),(b"S_VOBSUB","VobSub")):
        if marker in data:r.subtitles.append({"name":name,"format":name,"source":"codec marker in sample"})

def _required_element_end(data:bytes,pos:int)->int|None:
    if pos<0 or pos>=len(data):
        return None
    first=data[pos]
    mask=0x80
    id_len=1
    while id_len<=4 and not(first&mask):
        mask>>=1
        id_len+=1
    if id_len>4 or pos+id_len>=len(data):
        return None
    size_info=_vint(data,pos+id_len)
    if not size_info:
        return None
    size,size_len=size_info
    if size==(1<<(7*size_len))-1:
        return None
    return pos+id_len+size_len+size


async def inspect_telegram_message(client:Any,message:Any,progress:ProgressFn|None=None,deep:bool=True)->tuple[Report,int]:
    f=getattr(message,"file",None)
    total=getattr(f,"size",None)
    media=getattr(message,"media",None)
    if not media:
        raise ValueError("Message has no media")

    budget=deep_probe_budget()
    initial=min(initial_probe_bytes(),budget)
    parts=[]
    used=0

    async def say(s):
        if progress:
            await progress(s)

    await say("🧭 Stage 1/4 • reading the file header and container metadata…")
    b,err=await _read_range(client,media,total,0,initial)
    if not b:
        raise RuntimeError(err or "Telegram returned no probe bytes")
    parts.append(ProbePiece(0,b,"initial metadata"))
    used+=len(b)

    name=str(getattr(f,"name",None) or "telegram_file")
    mime=getattr(f,"mime_type",None)
    _,kind=magic(b,name,mime)

    if kind=="mkv":
        await say("🎯 Stage 2/4 • reading Matroska TrackEntry metadata…")
        targets=_seek_targets(b)
        if 0x1654AE6B not in targets:
            local=b.find(b"\x16\x54\xAE\x6B")
            if local>=0:
                targets[0x1654AE6B]=local
        if 0x1549A966 not in targets:
            local=b.find(b"\x15\x49\xA9\x66")
            if local>=0:
                targets[0x1549A966]=local
        for off,n,label in _probe_ranges(total,budget,initial,targets):
            x,_=await _read_range(client,media,total,off,n)
            if x:
                parts.append(ProbePiece(off,x,label))
                used+=len(x)
        await say("🧩 Stage 3/4 • extracting all detected video, audio and subtitle tracks…")
    elif kind=="mp4":
        await say("🧩 Stage 2/2 • reading MP4 metadata and bounded tail index…")
        if total and used<budget:
            tail_window=min(512*1024,budget-used,total)
            tail_offset=max(initial,total-tail_window)
            if tail_offset>=initial:
                x,_=await _read_range(client,media,total,tail_offset,tail_window)
                if x:
                    parts.append(ProbePiece(tail_offset,x,"MP4 tail metadata"))
                    used+=len(x)
    else:
        await say("🧩 Stage 2/2 • reading available media metadata…")

    await say("🧪 Stage 4/4 • assembling the final metadata report…")
    return _report(message,parts),used

def _safe(v:Any)->str:return html.escape(str(v))
def _rows(track:dict[str,Any],i:int):
    out=[f"{i}. {_safe(track.get('name') or track.get('display_name') or 'Unnamed')}"]
    if track.get("name_source"):out.append(f"   Name source: {_safe(track['name_source'])}")
    for k in ("track_uid","language_name","language","codec_name","codec","dimensions","display_dimensions","crop","frame_rate","scan_type","stereo_mode","alpha_mode","channels","sample_rate","output_sample_rate","bit_depth","codec_delay","seek_preroll","default","forced","enabled","original","commentary","hearing_impaired","visual_impaired"):
        if track.get(k):out.append(f"   {_safe(k.replace('_',' ').title())}: {_safe(track[k])}")
    return out

def format_report(r:Report)->str:
    a=r.audio.get("tracks",[]);v=r.video.get("tracks",[])
    lines=[
        "🔬 <b>FILE INTELLIGENCE</b>","",
        f"📄 <b>{_safe(r.filename)}</b>",
        f"📦 {_safe(r.detected)}",
        f"📏 {human(r.size)}",
        f"🧾 MIME: {_safe(r.mime or 'application/octet-stream')}",
    ]
    if r.container.get("runtime"):lines.append(f"⏱ Runtime: <b>{_safe(r.container['runtime'])}</b>")
    lines += [
        "",
        f"🎬 Video tracks: <b>{len(v) if isinstance(v,list) else 0}</b>",
        f"🔊 Audio tracks: <b>{len(a) if isinstance(a,list) else 0}</b>",
        f"💬 Subtitle tracks: <b>{len(r.subtitles)}</b>",
    ]
    if r.container.get("average_bitrate"):lines.append(f"⚙️ Average bitrate: {_safe(r.container['average_bitrate'])}")
    lines += ["",f"🧪 Sampled: {human(r.sampled)} across {len(r.probe_ranges)} targeted range(s)"]
    if r.notes:lines += ["",f"ℹ️ {_safe(r.notes[0])}"]
    return "\n".join(lines)


def format_section(r:Report,section:str)->str:
    if section=="audio":
        tracks=r.audio.get("tracks",[]);lines=["🔊 <b>AUDIO TRACKS</b>",""]
        if tracks:
            for i,t in enumerate(tracks,1):lines += _rows(t,i)+[""]
        else:
            lines.append("No confirmed audio track was exposed by the inspected metadata.")
            if r.audio.get("sample_codecs"):lines += ["",f"Codec marker(s): {_safe(r.audio['sample_codecs'])}"]
        return "\n".join(lines).strip()
    if section=="subs":
        lines=["💬 <b>SUBTITLE TRACKS</b>",""]
        if r.subtitles:
            for i,t in enumerate(r.subtitles,1):lines += _rows(t,i)+[""]
        else:lines.append("No confirmed subtitle track was exposed by the inspected metadata.")
        return "\n".join(lines).strip()
    if section=="video":
        tracks=r.video.get("tracks",[]);lines=["🎬 <b>VIDEO TRACKS</b>",""]
        if tracks:
            for i,t in enumerate(tracks,1):lines += _rows(t,i)+[""]
        else:lines.append("No confirmed video track was exposed by the inspected metadata.")
        return "\n".join(lines).strip()
    if section=="technical":
        lines=["⚙️ <b>TECHNICAL</b>","",f"Runtime: {_safe(r.container.get('runtime','Not available'))}",f"Container: {_safe(r.detected)}",f"MIME: {_safe(r.mime or 'application/octet-stream')}",f"Sampled: {human(r.sampled)}",f"Ranges: {len(r.probe_ranges)}"]
        if r.container.get("average_bitrate"):lines.append(f"Average bitrate: {_safe(r.container['average_bitrate'])}")
        if r.container.get("title"):lines.append(f"Container title: {_safe(r.container['title'])}")
        if r.container.get("muxing_app"):lines.append(f"Muxing app: {_safe(r.container['muxing_app'])}")
        if r.container.get("writing_app"):lines.append(f"Writing app: {_safe(r.container['writing_app'])}")
        if r.container.get("date_utc"):lines.append(f"Encoded date: {_safe(r.container['date_utc'])}")
        if r.probe_ranges:lines += ["","Probe map:"]+[f"• {_safe(x)}" for x in r.probe_ranges]
        return "\n".join(lines)
    return format_report(r)
