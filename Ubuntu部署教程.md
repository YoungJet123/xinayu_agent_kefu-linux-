# 闲鱼智能客服 - Ubuntu 部署教程

## 环境要求

- Ubuntu 桌面版（已在 22.04 验证）
- Python 3.8+
- MySQL 5.7+

---

## 一、安装系统依赖

```bash
sudo apt install python3-pip python3-tk mysql-server -y
```

启动 MySQL：

```bash
sudo systemctl start mysql
sudo systemctl enable mysql
```

---

## 二、安装 phpMyAdmin（可选，用于可视化管理数据库）

```bash
sudo apt install phpmyadmin libapache2-mod-php php-mbstring php-zip php-gd php-json php-curl -y
sudo systemctl restart apache2
```

安装过程中选择 apache2，配置数据库选 Yes。

设置 MySQL root 密码：

```bash
sudo mysql
```

```sql
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'your_password';
FLUSH PRIVILEGES;
EXIT;
```

浏览器访问 `http://localhost/phpmyadmin` 登录管理。

---

## 三、创建数据库

在 phpMyAdmin 中新建数据库，名称 `xbot`，字符集 `utf8mb4_general_ci`。

然后导入项目里的 SQL 文件：
- 点击 xbot 数据库 → 导入 → 选择 `xbot_20260117181948_backup.sql` → 执行

---

## 四、安装 Python 依赖

```bash
cd /path/to/xinayu_agent_kefu-main
pip3 install -r requirements.txt
~/.local/bin/playwright install chromium
```

---

## 五、配置 .env 文件

复制模板并编辑：

```bash
cp .env.example .env
```

必填项：

```
COZE_API_TOKEN=你的Coze API Token
COZE_BOT_ID=你的Bot ID
DB_PASSWORD=你的MySQL密码
DB_NAME=xbot
```

如果系统开启了 VPN 代理，在 .env 末尾加上：

```
NO_PROXY='*'
no_proxy='*'
```

---

## 六、启动

```bash
python3 gui.py
```

点击界面中的**启动**按钮，Chromium 浏览器会自动弹出，在浏览器中登录闲鱼账号即可。

登录状态会保存在 `browser_data/` 目录，下次启动无需重新登录。

---

## 常见问题

**浏览器没有弹出窗口**

确保 `gui.py` 中 `_run_handler` 方法设置了 DISPLAY 环境变量（项目已修复，无需手动处理）。

**Coze 连接失败，提示 socks proxy 错误**

系统代理导致，在 `.env` 中添加 `NO_PROXY='*'` 即可。

**phpMyAdmin 显示 PHP 源码**

```bash
sudo apt install libapache2-mod-php -y
sudo systemctl restart apache2
```
