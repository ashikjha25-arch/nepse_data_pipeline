1. Fork the repo

Go to the repo page → click Fork (creates your copy)

2. Clone to local: Select a folder where you want to store the project, then in the cmd line write each line one-by-one

git clone https://github.com/caster-k/nepse_data_pipeline.git
cd nepse_data_pipeline

3. Create a branch: After you are inside nepse folder, again in the cmd, type out the following command

git checkout -b your-branch-name

NOTE: within the cmd type 'git status' and make sure the output mention's the branch name you specified

4. Make changes + commit: Once you make your changes, add the changes and commit them for review and merge

git add .
git commit -m "Describe your changes"

5. Push branch

git push origin your-branch-name

6. Open a Pull Request

Go to your fork on GitHub → click Compare & pull request → submit for review