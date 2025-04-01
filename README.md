# Watch
**Watch Files and Do Stuff!**

## Example
**Log Kafka messages and ignore the generated confirmation files**
```
python watch.py --directory ./logs --kafka-endpoint localhost:9002 --pattern '^(?!.*\.kafka$).*$'
```
