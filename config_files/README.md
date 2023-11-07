# Configurations

## Scripts

- `json_to_yaml.py`

## Settings

- `.zshrc`: oh-my-zsh configuration to be placed at `~/`
- `wedisagree.zsh-theme`: oh-my-zsh theme settings to be placed at `~/oh-my-zsh/themes/`
- `git gone`
  - In the terminal run:

    ```
    git config --global alias.gone "! git fetch -p && git for-each-ref --format '%(refname:short) %(upstream:track)' | awk '\$2 == \"[gone]\" {print \$1}' | xargs -r git branch -D"
    ```
    Reference: https://www.erikschierboom.com/2020/02/17/cleaning-up-local-git-branches-deleted-on-a-remote/
