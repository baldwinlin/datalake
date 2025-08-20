# Git 指令參考手冊

## 1. 跳過追蹤配置文件

### 暫時不追蹤 main.conf 文件（避免誤推送敏感配置）

```bash
git update-index --skip-worktree conf/main.conf
```

### 重新開始追蹤 main.conf 文件（需要上傳時使用）

```bash
git update-index --no-skip-worktree conf/main.conf
```

### 檢查哪些文件被跳過追蹤

```bash
git ls-files -v | grep "^S"
```

---

## 2. 創建分支並推送更新

### 創建並切換到新分支

```bash
# 使用 checkout（傳統方式）
git checkout -b <branch-name>

# 使用 switch（推薦，Git 2.23+）
git switch -c <branch-name>
```

### 切換到現有分支

```bash
# 使用 checkout（傳統方式）
git checkout <branch-name>

# 使用 switch（推薦，Git 2.23+）
git switch <branch-name>
```

### 添加變更到暫存區

```bash
git add .
# 或指定特定文件
git add <file-name>
```

### 提交變更

```bash
git commit -m "描述你的變更內容"
```

### 推送新分支到遠端倉庫

```bash
git push origin <branch-name>
```

### 推送現有分支的更新

```bash
git push
```

---

## 3. 更新本地 master 分支

### 切換回 master 分支

```bash
# 使用 switch（推薦）
git switch master

# 使用 checkout（傳統方式）
git checkout master
```

### 拉取遠端 master 最新版本

```bash
# 快進模式（推薦，保持線性歷史）
git pull --ff-only origin master

# 一般模式（可能產生合併提交）
git pull origin master

# 或簡化版本（如果已設定追蹤分支）
git pull --ff-only
git pull
```

### 或者使用 fetch + merge 方式

```bash
git fetch origin
git merge origin/master
```

### 刪除已合併的本地分支（可選）

```bash
git branch -d <branch-name>
```

---

## 常用輔助指令

### 查看當前分支狀態

```bash
git status
```

### 查看分支列表

```bash
git branch
git branch -a  # 包含遠端分支
```

### 查看提交歷史

```bash
git log --oneline
```

### 查看遠端倉庫資訊

```bash
git remote -v
```

---

## checkout vs switch 差異說明

### git checkout（傳統指令）

- **多功能**：分支切換 + 文件恢復 + 創建分支
- **語法範例**：
  ```bash
  git checkout <branch-name>     # 切換分支
  git checkout -b <branch-name>  # 創建並切換分支
  git checkout <file-name>       # 恢復文件（危險操作）
  git checkout <commit-hash>     # 切換到特定提交
  ```

### git switch（專用分支指令，Git 2.23+）

- **專一功能**：只負責分支切換和創建
- **更安全**：不會意外恢復文件
- **語法範例**：
  ```bash
  git switch <branch-name>       # 切換分支
  git switch -c <branch-name>    # 創建並切換分支
  git switch -                   # 切換到上一個分支
  ```

### git restore（專用文件恢復指令，Git 2.23+）

- **專門恢復文件**：取代 checkout 的文件恢復功能
- **語法範例**：
  ```bash
  git restore <file-name>        # 恢復工作區文件
  git restore --staged <file-name>  # 恢復暫存區文件
  ```

**建議**：新專案建議使用 `git switch` 和 `git restore`，語意更清楚且更安全。

---

## pull vs pull --ff-only 差異說明

### git pull（一般模式）

- **行為**：自動合併遠端變更到本地分支
- **合併策略**：
  ```bash
  git pull origin master
  # 等同於：
  git fetch origin
  git merge origin/master
  ```
- **可能結果**：
  - 如果本地沒有新提交：快進合併（fast-forward）
  - 如果本地有新提交：創建合併提交（merge commit）
- **提交歷史**：可能產生分叉的提交圖

### git pull --ff-only（快進模式）

- **行為**：只有在可以快進合併時才會更新
- **嚴格條件**：
  ```bash
  git pull --ff-only origin master
  ```
- **可能結果**：
  - 成功：當本地分支是遠端分支的直接祖先
  - 失敗：當本地有未推送的提交時會報錯
- **提交歷史**：始終保持線性歷史

### 使用場景建議

**使用 `--ff-only` 當：**

- 你確定本地沒有未推送的提交
- 想要保持乾淨的線性提交歷史
- 在 master/main 分支上更新（推薦）

**使用一般 `pull` 當：**

- 在功能分支上工作
- 需要合併遠端變更與本地變更

**錯誤處理：**

```bash
# 如果 --ff-only 失敗，表示本地有未推送的提交
# 可以選擇：
git rebase origin/master  # 重新整理本地提交
# 或
git merge origin/master   # 創建合併提交
```

---

## 完整工作流程範例

```bash
# 1. 跳過追蹤配置文件
git update-index --skip-worktree conf/main.conf

# 2. 創建新分支進行開發
git switch -c feature/new-feature

# 3. 進行開發工作...

# 4. 提交變更
git add .
git commit -m "Add new feature"

# 5. 推送分支
git push origin feature/new-feature

# 6. 等待 SA merge 到 master 後，更新本地
git switch master
git pull --ff-only origin master

# 7. 清理已合併的分支
git branch -d feature/new-feature
```
