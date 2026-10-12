# HW1: HDFS Cluster

## Требования
- 1 NameNode
- 1 SecondaryNameNode
- 3 DataNode

## Установка
Запускать на edge из корня репы.

```bash
cp shared/inventory.example.ini shared/inventory.ini
ansible-playbook -i shared/inventory.ini hw01-hdfs/ansible/deploy.yml
```

deploy.yml ставит java и hadoop, раскидывает конфиги, форматирует namenode (только если он пустой) и поднимает namenode, secondary namenode и 3 datanode. Можно запускать повторно, данные не трутся.

Остановить:

```bash
ansible-playbook -i shared/inventory.ini hw01-hdfs/ansible/stop.yml
```

Проверить: `ssh -N -L 9870:team-37-nn:9870 team@178.236.27.186`, потом открыть localhost:9870, там должно быть 3 живых datanode.

## Проверка

QA: [диагностика HDFS и безопасный smoke-test](QA.md).
