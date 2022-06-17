# What is this?

This layer just adds some GDK stuff so we can publish to our accounts.


## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Updating

1. "Simply" rebase the branch `addTargetPayloadTemplate` onto `upstream/main`
2. `mvn test` (on my PC the `MQTTClientKeyStoreTest.java` tests fail even on the clean repo, so if that's the only failure, then this patch is still good -- the patch has tests that test it)
2. `git push -f`
3. Rebase `bdc-custom-component` onto `addTargetPayloadTemplate`
4. `git push -f`
  > if the only thing changes is the version number, I've just been ammending the single commit to keep it cleaner

## Building & Publishing

1. make sure it builds

  ```bash
  gdk component build
  ```

2. publish

  ```bash
  AWS_PROFILE=AdministratorAccess-513038817248 python scripts/deploy.py --version 1.1.0
  ```

3. update usages to the new version as applicable (the old one is still available for use)
