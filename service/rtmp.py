import asyncio
import json
import os
import subprocess
import sys
import threading
import time
from collections import OrderedDict
from functools import lru_cache

import utils.constants as constants
from utils.config import config
from utils.db import get_db_connection, return_db_connection
from utils.ffmpeg import probe_url
from utils.i18n import t
from utils.tools import join_url, resource_path, render_nginx_conf

nginx_dir = resource_path(os.path.join('utils', 'nginx-rtmp-win32'))
nginx_conf_template = resource_path(os.path.join(nginx_dir, 'conf', 'nginx.conf.template'))
nginx_conf = resource_path(os.path.join(nginx_dir, 'conf', 'nginx.conf'))
nginx_path = resource_path(os.path.join(nginx_dir, 'nginx.exe'))
stop_path = resource_path(os.path.join(nginx_dir, 'stop.bat'))
app_rtmp_url = f"rtmp://127.0.0.1:{config.nginx_rtmp_port}"

hls_running_streams = OrderedDict()
STREAMS_LOCK = threading.Lock()
hls_last_access = {}
HLS_IDLE_TIMEOUT = config.rtmp_idle_timeout
HLS_WAIT_TIMEOUT = 30
HLS_WAIT_INTERVAL = 0.5
MAX_STREAMS = config.rtmp_max_streams
nginx_dir = resource_path(os.path.join('utils', 'nginx-rtmp-win32'))
hls_temp_path = resource_path(os.path.join(nginx_dir, 'temp', 'hls')) if sys.platform == "win32" else '/tmp/hls'

_hls_monitor_started_evt = threading.Event()
_hls_monitor_lock = threading.Lock()


def _is_copy_compatible(meta: dict) -> bool:
    """
    Decide whether stream copy is safe.
    """
    try:
        if not meta:
            return False

        def _norm(name: str | None, is_video: bool = True) -> str | None:
            if not name:
                return None
            n = name.lower()
            video_aliases = {
                'h265': 'hevc',
                'x265': 'hevc',
                'h.265': 'hevc',
                'avc1': 'h264',
            }
            audio_aliases = {
                'mpa': 'mp2',
                'mpeg2': 'mp2',
                'aac_latm': 'aac',
                'ac-3': 'ac3',
                'eac3': 'ac3',
            }
            if is_video:
                return video_aliases.get(n, n)
            else:
                return audio_aliases.get(n, n)

        video_codec = _norm(meta.get('video_codec'), is_video=True)
        audio_codec = _norm(meta.get('audio_codec'), is_video=False)

        browser_video = {"h264", "avc1"}
        browser_audio = {"aac", "mp3"}

        player_video = {"h264", "hevc", "vp8", "vp9", "av1", "mpeg2", "mpeg1"}
        player_audio = {"aac", "mp3", "mp2", "ac3", "opus", "vorbis"}

        if video_codec and video_codec in browser_video and (audio_codec is None or audio_codec in browser_audio):
            return True

        if video_codec and video_codec in player_video and (audio_codec is None or audio_codec in player_audio):
            if meta.get('force_player_copy') or meta.get('extra_info') and 'force_player_copy' in str(
                    meta.get('extra_info')):
                return True
            return False
        return False
    except Exception:
        pass
    return False


def _save_probe_metadata_to_db(channel_id: str, url: str, headers: dict | None, meta: dict | None):
    """
    Save probe metadata into result_data table (create full schema if needed).
    """
    if not meta:
        return
    try:
        conn = get_db_connection(constants.rtmp_data_path)
    except Exception as e:
        print(t("msg.write_error").format(info=f"open rtmp db error: {e}"))
        return
    try:
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS result_data ("
            "id TEXT PRIMARY KEY, url TEXT, headers TEXT, video_codec TEXT, audio_codec TEXT, resolution TEXT, fps REAL)"
        )
        cursor.execute(
            "INSERT OR REPLACE INTO result_data (id, url, headers, video_codec, audio_codec, resolution, fps) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                str(channel_id),
                url,
                json.dumps(headers) if headers else None,
                meta.get('video_codec'),
                meta.get('audio_codec'),
                meta.get('resolution'),
                meta.get('fps')
            )
        )
        conn.commit()
    except Exception:
        pass
    finally:
        return_db_connection(constants.rtmp_data_path, conn)


def ensure_hls_idle_monitor_started():
    if _hls_monitor_started_evt.is_set():
        return
    with _hls_monitor_lock:
        if _hls_monitor_started_evt.is_set():
            return
        try:
            if not config.open_rtmp:
                return
            thread = threading.Thread(target=hls_idle_monitor, daemon=True, name="hls-idle-monitor")
            thread.start()
            _hls_monitor_started_evt.set()
            print(t("msg.rtmp_hls_idle_monitor_start_success"))
        except Exception as e:
            print(t("msg.rtmp_hls_idle_monitor_start_fail").format(info=e))


@lru_cache(maxsize=1)
def _get_video_encoder_args():
    """
    Get the best available video encoder arguments based on the system's ffmpeg encoders.
    """
    preferred = ['h264_nvenc', 'h264_qsv', 'h264_amf', 'libx264']

    try:
        res = subprocess.run(['ffmpeg', '-hide_banner', '-encoders'],
                             capture_output=True, text=True, timeout=10)
        enc_list = res.stdout
    except Exception:
        enc_list = ''

    for enc in preferred:
        if enc in enc_list:
            return ['-c:v', enc, '-preset', 'veryfast']

    return ['-c:v', 'libx264', '-preset', 'veryfast']


def invalidate_video_encoder_args_cache():
    """
    Invalidate the cached video encoder arguments, forcing a re-check of available encoders on next use.
    """
    try:
        _get_video_encoder_args.cache_clear()
    except Exception:
        pass


@lru_cache(maxsize=1)
def _get_video_encoder_candidates():
    """
    Probe ffmpeg for available encoders and return a list of encoder argument lists in preferred order.
    This is used to try fallbacks when a chosen encoder fails at runtime.
    """
    preferred = ['h264_nvenc', 'h264_qsv', 'h264_amf', 'libx264']
    candidates = []
    try:
        res = subprocess.run(['ffmpeg', '-hide_banner', '-encoders'], capture_output=True, text=True, timeout=10)
        enc_list = res.stdout or ''
    except Exception:
        enc_list = ''

    for enc in preferred:
        if enc in enc_list:
            candidates.append(['-c:v', enc, '-preset', 'veryfast'])

    if not any('libx264' in ' '.join(c) for c in candidates):
        candidates.append(['-c:v', 'libx264', '-preset', 'veryfast'])

    return candidates


def start_hls_to_rtmp(host, channel_id):
    ensure_hls_idle_monitor_started()

    if not host:
        return None
    if not channel_id:
        return print(t("msg.error_channel_id_not_found"))

    data = get_channel_data(channel_id)
    url = data.get("url", "")
    if not url:
        return print(t("msg.error_channel_url_not_found"))

    with STREAMS_LOCK:
        if channel_id in hls_running_streams:
            process = hls_running_streams[channel_id]
            if process.poll() is None:
                return print(t("msg.rtmp_hls_stream_already_running"))
            else:
                del hls_running_streams[channel_id]

    cleanup_streams(hls_running_streams)

    headers = data.get("headers", None)
    headers_str = ''.join(f'{k}: {v}\r\n' for k, v in headers.items()) if headers else ''

    meta = {
        'video_codec': data.get('video_codec'),
        'audio_codec': data.get('audio_codec'),
        'resolution': data.get('resolution'),
        'fps': data.get('fps'),
    }

    if not meta.get('video_codec') and not meta.get('audio_codec') and config.open_rtmp:
        try:
            probed = None
            try:
                probed = asyncio.run(probe_url(url, headers, timeout=10))
            except RuntimeError:
                loop = asyncio.new_event_loop()
                try:
                    probed = loop.run_until_complete(probe_url(url, headers, timeout=10))
                finally:
                    try:
                        loop.close()
                    except Exception:
                        pass
            if probed:
                meta.update(probed)
                _save_probe_metadata_to_db(channel_id, url, headers, probed)
        except Exception:
            pass

    use_copy = _is_copy_compatible(meta)

    base_cmd = [
        'ffmpeg',
        '-loglevel', 'error',
        '-re',
    ]

    local_loop = False
    try:
        parsed_url = url.partition('$')[0]
        if parsed_url.startswith('file://'):
            local_path = parsed_url[len('file://'):]
            if os.path.exists(local_path) and not local_path.lower().endswith('.m3u8'):
                local_loop = True
                url_input = local_path
            else:
                url_input = parsed_url
        else:
            url_input = parsed_url
            if os.path.exists(url_input) and not url_input.lower().endswith('.m3u8'):
                local_loop = True
    except Exception:
        url_input = url.partition('$')[0]

    if headers_str and not local_loop:
        base_cmd += ['-headers', headers_str]

    if local_loop:
        base_cmd += ['-stream_loop', '-1']

    base_cmd += ['-i', url_input]

    if use_copy:
        cmd = base_cmd + [
            '-c:v', 'copy',
            '-c:a', 'copy',
            '-f', 'flv',
            '-flvflags', 'no_duration_filesize',
            join_url(host, channel_id)
        ]
        target_video_codec = meta.get('video_codec') or t('name.unknown')
        target_audio_codec = meta.get('audio_codec') or t('name.unknown')
        target_mode = 'copy'
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL
            )
            print(
                t("msg.rtmp_publish").format(channel_id=channel_id, source=url)
                + f", {t('name.fps')}: {meta.get('fps') or t('name.unknown')}, [{target_mode}]: {t('name.video_codec')}: {target_video_codec}, {t('name.audio_codec')}: {target_audio_codec}"
            )
        except Exception as e:
            return print(t("msg.error_start_ffmpeg_failed").format(info=e))

        threading.Thread(
            target=monitor_stream_process,
            args=(hls_running_streams, process, channel_id),
            daemon=True
        ).start()

        with STREAMS_LOCK:
            hls_running_streams[channel_id] = process
        return

    candidates = _get_video_encoder_candidates()
    rest_args = [
        '-c:a', 'aac',
        '-b:a', '128k',
        '-f', 'flv',
        '-flvflags', 'no_duration_filesize',
        join_url(host, channel_id)
    ]

    process = None
    chosen_encoder = None
    grace_seconds = 3

    for enc_args in candidates:
        enc_name = enc_args[1]
        print(t("msg.rtmp_try_encoder").format(encoder=enc_name, channel_id=channel_id))
        cmd_try = base_cmd + enc_args + rest_args
        try:
            p = subprocess.Popen(
                cmd_try,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL
            )
        except Exception as e:
            print(t("msg.rtmp_encoder_start_failed").format(encoder=enc_name, info=e))
            continue

        start_time = time.time()
        succeeded = True
        while time.time() - start_time < grace_seconds:
            if p.poll() is not None:
                succeeded = False
                break
            time.sleep(0.2)

        if succeeded and p.poll() is None:
            process = p
            chosen_encoder = enc_name
            break
        else:
            try:
                _terminate_process_safe(p)
            except Exception:
                pass
            print(t("msg.rtmp_encoder_quick_fail").format(encoder=enc_name))

    if not process:
        all_failed_msg = t("msg.rtmp_all_encoders_failed")
        return print(t("msg.error_start_ffmpeg_failed").format(info=all_failed_msg))

    target_video_codec = chosen_encoder or 'libx264'
    target_audio_codec = 'aac'
    target_mode = 'transcode'

    print(
        t("msg.rtmp_publish").format(channel_id=channel_id, source=url)
        + f", {t('name.fps')}: {meta.get('fps') or t('name.unknown')}, [{target_mode}]: {t('name.video_codec')}: {target_video_codec}, {t('name.audio_codec')}: {target_audio_codec}"
    )

    threading.Thread(
        target=monitor_stream_process,
        args=(hls_running_streams, process, channel_id),
        daemon=True
    ).start()

    with STREAMS_LOCK:
        hls_running_streams[channel_id] = process


def _terminate_process_safe(process):
    try:
        process.terminate()
        process.wait(timeout=5)
    except Exception:
        try:
            process.kill()
            process.wait(timeout=5)
        except Exception:
            pass


def cleanup_streams(streams):
    with STREAMS_LOCK:
        to_delete = []
        for channel_id, process in list(streams.items()):
            if process.poll() is not None:
                to_delete.append(channel_id)
        for channel_id in to_delete:
            streams.pop(channel_id, None)

        while len(streams) > MAX_STREAMS:
            try:
                oldest_channel_id, oldest_proc = streams.popitem(last=False)
                _terminate_process_safe(oldest_proc)
            except KeyError:
                break


def monitor_stream_process(streams, process, channel_id):
    try:
        process.wait()
    except Exception:
        pass
    with STREAMS_LOCK:
        if channel_id in streams and streams[channel_id] is process:
            del streams[channel_id]


def hls_idle_monitor():
    while True:
        now = time.time()
        to_stop = []

        with STREAMS_LOCK:
            for channel_id, last_ts in list(hls_last_access.items()):
                proc = hls_running_streams.get(channel_id)
                if proc and proc.poll() is None:
                    if now - last_ts > HLS_IDLE_TIMEOUT:
                        print(t("msg_rtmp_hls_idle_will_stop").format(channel_id=channel_id,
                                                                      second=f"{now - last_ts:.1f}"))
                        to_stop.append(channel_id)

        for cid in to_stop:
            stop_stream(cid)
            with STREAMS_LOCK:
                hls_last_access.pop(cid, None)

        time.sleep(5)


def get_channel_data(channel_id):
    conn = get_db_connection(constants.rtmp_data_path)
    channel_data = {}
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT url, headers, video_codec, audio_codec, resolution, fps FROM result_data WHERE id=?",
            (channel_id,))
        data = cursor.fetchone()
        if data:
            channel_data = {
                'url': data[0],
                'headers': json.loads(data[1]) if data[1] else None,
                'video_codec': data[2],
                'audio_codec': data[3],
                'resolution': data[4],
                'fps': data[5]
            }
    except Exception as e:
        print(t("msg.error_get_channel_data_from_database").format(info=e))
    finally:
        return_db_connection(constants.rtmp_data_path, conn)
    return channel_data


def stop_stream(channel_id):
    with STREAMS_LOCK:
        process = hls_running_streams.get(channel_id)
        if process and process.poll() is None:
            try:
                _terminate_process_safe(process)
            except Exception as e:
                print(t("msg.error_stop_channel_stream").format(channel_id=channel_id, info=e))
        hls_running_streams.pop(channel_id, None)


def start_rtmp_service():
    render_nginx_conf(nginx_conf_template, nginx_conf)
    original_dir = os.getcwd()
    try:
        os.chdir(nginx_dir)
        subprocess.Popen([nginx_path], shell=True)
    except Exception as e:
        print(t("msg.error_rtmp_service_start_failed").format(info=e))
    finally:
        os.chdir(original_dir)


def stop_rtmp_service():
    try:
        os.chdir(nginx_dir)
        subprocess.Popen([stop_path], shell=True)
    except Exception as e:
        print(t("msg.error_rtmp_service_stop_failed").format(info=e))
