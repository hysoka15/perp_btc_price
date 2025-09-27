# Tx


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**hash** | **str** |  | 
**type** | **int** |  | 
**info** | **str** |  | 
**event_info** | **str** |  | 
**status** | **int** |  | 
**transaction_index** | **int** |  | 
**l1_address** | **str** |  | 
**account_index** | **int** |  | 
**nonce** | **int** |  | 
**expire_at** | **int** |  | 
**block_height** | **int** |  | 
**queued_at** | **int** |  | 
**executed_at** | **int** |  | 
**sequence_index** | **int** |  | 
**parent_hash** | **str** |  | 

## Example

```python
from lighter.models.tx import Tx

# TODO update the JSON string below
json = "{}"
# create an instance of Tx from a JSON string
tx_instance = Tx.from_json(json)
# print the JSON string representation of the object
print(Tx.to_json())

# convert the object into a dict
tx_dict = tx_instance.to_dict()
# create an instance of Tx from a dict
tx_from_dict = Tx.from_dict(tx_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


