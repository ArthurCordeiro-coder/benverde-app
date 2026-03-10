from git import Repo

repo = Repo(r"C:\Users\pesso\benverde-app")

for commit in repo.iter_commits():
    print(f"{str(commit.hexsha)[:7]} | {commit.authored_datetime} | {commit.author} | {commit.message.strip()}")