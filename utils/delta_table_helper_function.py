from delta.tables import DeltaTable


def upsert_to_delta(batch_df,batch_id,query:str,path:str,is_silver:bool,partition_cols:[]=None):
    '''
    Function to merge to delta usign the .merge() for silver others go same 
    Takes query as a string parameter for the query to match upon 
    ,path as a string to configure the write location
    ,is_silver to identify to know if silver table then use merge
    ,partiton_col as columns to add in partitionBy incase of silver required 
    gold and bronze not required there will be no parttion in case the partition cols are not 
    provided
    '''
    if is_silver:
        #try to also work when table does not exit
        
        try:
            delta_table=DeltaTable.forPath(batch_df.sparkSession,path)
            delta_table.alias("target").merge(batch_df.alias("source"),query).whenNotMatchedInsertAll().execute()
        except:
            batch_df.write.format("delta").partitionBy(*partition_cols).save(path)
    else:
        if partition_cols:
            batch_df.write.format("delta").partitionBy(*partition_cols).mode("append").save(path)
        else:
            batch_df.write.format("delta").mode("append").save(path)