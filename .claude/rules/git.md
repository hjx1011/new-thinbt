# Git 规范

## 提交
- 不要自动提交，除非我明确要求
- commit message 用中文写
- push 前先确认

## 分支
- 新功能在独立分支开发，不要直接在 main 上改
- main 是主分支，只接受合并
- 分支命名：`feature/xxx`、`fix/xxx`、`refactor/xxx`

## 禁止操作
- 不要 `git push --force`
- 不要 `git reset --hard` 除非我明确确认
- 不要在 main 上直接 commit
