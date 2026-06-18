#!/usr/bin/env python3
"""
run_app.py —— 大数据分析看板 一键启动脚本 (Milestone 4)
=========================================================
功能:
  1. 环境自检 —— 检查必要数据文件和端口可用性
  2. 异步进程管理 —— 使用 subprocess 启动 FastAPI 服务
  3. 自动浏览器唤起 —— 服务就绪后自动打开前端页面
  4. 优雅终止 —— Ctrl+C 时清理子进程，不留孤儿进程

用法:
  python run_app.py
  python run_app.py --port 8000 --no-browser
"""

import argparse
import os
import signal
import socket
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

# 加载 .env 文件中的环境变量
from dotenv import load_dotenv
for env_path in [Path(__file__).resolve().parent / ".env"]:
    if env_path.exists():
        load_dotenv(env_path)
        break

# ============================================================
# 全局配置
# ============================================================
BASE_DIR = Path(__file__).resolve().parent

# 使用实验十三的最新增强版 dashboard（5个API端点 + 完整前端交互）
DASHBOARD_DIR = BASE_DIR / "实验十三" / "dashboard"
SERVER_FILE = DASHBOARD_DIR / "server.py"

# 必要的数据文件（至少存在一个即可）
REQUIRED_DATA_FILES = [
    BASE_DIR / "实验九" / "data" / "online_shopping_10_cats.csv",
    BASE_DIR / "实验十一" / "online_shopping_10_cats (1).csv",
    BASE_DIR / "实验十" / "batch_1000_features.csv",
]

# 大模型 API Key 环境变量名列表（用于启动前检测）
LLM_API_KEY_NAMES = [
    "SILICONFLOW_API_KEY",
    "DASHSCOPE_API_KEY",
    "DEEPSEEK_API_KEY",
    "OPENAI_API_KEY",
    "MOONSHOT_API_KEY",
]

# 启动超时（秒）
STARTUP_TIMEOUT = 15
# 轮询间隔（秒）
POLL_INTERVAL = 0.5


# ============================================================
# 终端颜色输出
# ============================================================
class Colors:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


def print_header(msg: str):
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}  {msg}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*60}{Colors.RESET}")


def print_ok(msg: str):
    print(f"  {Colors.GREEN}[OK]{Colors.RESET} {msg}")


def print_warn(msg: str):
    print(f"  {Colors.YELLOW}[WARN]{Colors.RESET} {msg}")


def print_err(msg: str):
    print(f"  {Colors.RED}[ERR]{Colors.RESET} {msg}")


def print_info(msg: str):
    print(f"  {Colors.BLUE}[INFO]{Colors.RESET} {msg}")


# ============================================================
# 环境自检
# ============================================================
def check_port_available(host: str, port: int) -> bool:
    """检查指定端口是否可用"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        try:
            sock.bind((host, port))
            return True
        except OSError:
            return False


def check_data_files() -> bool:
    """检查是否存在至少一个数据文件"""
    found = [f for f in REQUIRED_DATA_FILES if f.exists()]
    if found:
        for f in found:
            size_mb = f.stat().st_size / (1024 * 1024)
            print_ok(f"数据文件存在: {f.name} ({size_mb:.1f} MB)")
        return True
    return False


def check_api_keys() -> dict:
    """检测大模型 API Key 配置状态"""
    result = {"configured": [], "missing": []}
    for key_name in LLM_API_KEY_NAMES:
        if os.environ.get(key_name):
            result["configured"].append(key_name)
        else:
            result["missing"].append(key_name)
    return result


def check_dashboard_dir() -> bool:
    """检查 dashboard 目录及必要文件"""
    if not SERVER_FILE.exists():
        print_err(f"找不到 server.py: {SERVER_FILE}")
        return False
    print_ok(f"Dashboard 服务文件: {SERVER_FILE}")
    frontend_dir = DASHBOARD_DIR / "frontend"
    if frontend_dir.exists() and (frontend_dir / "index.html").exists():
        print_ok(f"前端页面: {frontend_dir / 'index.html'}")
    return True


def run_preflight_checks(port: int) -> bool:
    """执行所有启动前检查"""
    print_header("STEP 1/4 · 环境自检")

    all_ok = True

    # 1. 检查数据文件
    if not check_data_files():
        print_err("未找到任何数据文件！系统将以降级模式启动（部分功能可能不可用）")
        # 不阻止启动，允许降级运行

    # 2. 检查端口
    if not check_port_available("127.0.0.1", port):
        print_err(f"端口 {port} 已被占用！请先关闭占用进程或更换端口")
        print_info(f"排查命令: netstat -ano | findstr :{port}")
        return False
    print_ok(f"端口 {port} 可用")

    # 3. 检查 Dashboard 文件
    if not check_dashboard_dir():
        return False

    # 4. 检查 API Key 配置（只需至少一个即可）
    api_status = check_api_keys()
    if api_status["configured"]:
        print_ok(f"已配置 API Key: {', '.join(api_status['configured'])}")
    else:
        print_warn("未配置任何大模型 API Key")
        print_info("大模型功能将降级运行（使用内置规则库），看板顶部将显示降级提示")

    # 5. 检查虚拟环境
    venv_path = BASE_DIR / "data_env"
    if venv_path.exists():
        print_ok(f"虚拟环境: {venv_path}")
    else:
        print_warn("未检测到虚拟环境，请确保依赖已安装: pip install -r requirements.txt")

    print()
    return all_ok


# ============================================================
# 启动服务 & 等待就绪
# ============================================================
def start_server(port: int) -> subprocess.Popen:
    """启动 uvicorn 子进程"""
    print_header("STEP 2/4 · 启动 FastAPI 服务")

    cmd = [
        sys.executable, "-m", "uvicorn",
        "server:app",
        "--host", "127.0.0.1",
        "--port", str(port),
    ]

    # Windows 上 subprocess 创建独立进程组，便于 Ctrl+C 时统一清理
    if os.name == "nt":
        proc = subprocess.Popen(
            cmd,
            cwd=str(DASHBOARD_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
        )
    else:
        proc = subprocess.Popen(
            cmd,
            cwd=str(DASHBOARD_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            preexec_fn=os.setsid,
        )

    print_info(f"uvicorn 进程已启动 (PID: {proc.pid})")
    return proc


def wait_for_server(host: str, port: int, timeout: int) -> bool:
    """轮询等待服务就绪"""
    print_header("STEP 3/4 · 等待服务就绪")

    url = f"http://{host}:{port}/api/category-distribution"
    start = time.time()
    dots = 0

    while time.time() - start < timeout:
        try:
            import urllib.request
            req = urllib.request.Request(url)
            urllib.request.urlopen(req, timeout=2)
            elapsed = time.time() - start
            print()
            print_ok(f"服务就绪！(耗时 {elapsed:.1f}s)")
            print_info(f"  API 文档:  http://{host}:{port}/docs")
            print_info(f"  前端看板:  http://{host}:{port}/")
            return True
        except Exception:
            dots = (dots + 1) % 4
            print(f"\r  等待服务启动{'.' * dots}  ", end="", flush=True)
            time.sleep(POLL_INTERVAL)

    print()
    print_err(f"服务启动超时（{timeout}s），请检查控制台是否有错误日志")
    return False


# ============================================================
# 打开浏览器
# ============================================================
def open_browser(port: int):
    """自动打开默认浏览器"""
    print_header("STEP 4/4 · 打开浏览器")
    url = f"http://127.0.0.1:{port}"
    print_info(f"正在打开: {url}")
    webbrowser.open(url)
    print_ok("浏览器已打开（如未自动弹出，请手动访问上方地址）")
    print()


# ============================================================
# 优雅终止
# ============================================================
def setup_graceful_shutdown(proc: subprocess.Popen):
    """注册 Ctrl+C 优雅终止处理器"""
    def shutdown_handler(signum, frame):
        print()
        print_header("正在关闭服务...")
        try:
            if os.name == "nt":
                # Windows: 终止整个进程树
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                    capture_output=True,
                )
            else:
                # Unix: 发送 SIGTERM 到进程组
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                time.sleep(1)
                # 如果还没退出，强制 SIGKILL
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except OSError:
                    pass

            print_ok("子进程已清理完毕")
        except Exception as e:
            print_warn(f"清理子进程时出现问题: {e}")
            print_info(f"可手动清理: taskkill /F /PID {proc.pid}")

        print_ok("系统已关闭，再见！")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


# ============================================================
# 主入口
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="大数据分析看板 · 一键启动脚本 (M4 交付系统)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_app.py                     # 默认端口 8000，自动打开浏览器
  python run_app.py --port 8080         # 自定义端口
  python run_app.py --no-browser        # 不自动打开浏览器
        """,
    )
    parser.add_argument("--port", type=int, default=8000, help="监听端口 (默认: 8000)")
    parser.add_argument("--no-browser", action="store_true", help="不自动打开浏览器")
    args = parser.parse_args()

    print_header("大数据分析看板 · 一键启动 (Milestone 4)")

    # Step 1: 环境自检
    if not run_preflight_checks(args.port):
        print_err("环境检查未通过，启动终止")
        print_info("请根据上方 [ERR] 提示修复问题后重新运行")
        sys.exit(1)

    # Step 2: 启动服务
    proc = start_server(args.port)

    # 注册优雅终止
    setup_graceful_shutdown(proc)

    # Step 3: 等待就绪
    if not wait_for_server("127.0.0.1", args.port, STARTUP_TIMEOUT):
        print_warn("服务可能未正常启动，请查看上方日志")
        # 不强制退出，让用户能看到错误信息
        print_info("按 Ctrl+C 退出...")

    # Step 4: 打开浏览器
    if not args.no_browser:
        open_browser(args.port)

    # 持续运行，实时打印子进程输出
    print_header("系统运行中 · 按 Ctrl+C 关闭")
    try:
        while True:
            line = proc.stdout.readline()
            if line:
                # 只打印关键日志，过滤重复信息
                stripped = line.strip()
                if stripped and any(
                    kw in stripped
                    for kw in ["Error", "error", "WARNING", "ERROR", "started", "listening"]
                ):
                    print(f"  [uvicorn] {stripped}")
            elif proc.poll() is not None:
                print_err(f"uvicorn 进程意外退出 (exit code: {proc.returncode})")
                break
    except KeyboardInterrupt:
        pass  # signal handler 会处理
    except Exception as e:
        print_err(f"运行时异常: {e}")


if __name__ == "__main__":
    main()
