from pyspark.sql import SparkSession
from spark_singleton import SparkSingleton
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import random 

class Transaction:
    """
        This class generates a transation object.

        Transaction: payment_type, payment_txn_id, payment_txn_success, failure_reason

    """
    #global 
    txn_id_list = []

    def __init__(self):

        # artificial trend- percentage of payments that are 'card'
        # self.__cardTrend = 0.4
        # self.__txnSuccessRate = 0.95

        self.__paymentTxnId = self.genTxnId()
        self.__paymentType = self.genTxnType()
        self.__paymentTxnSuccess = self.genTxnSuccess()
        self.__failureReason = self.genFailureReason()

        # print("Transaction created.")


    def genTxnId(self):
        """
            Generate a unique 8 digit transaction ID.
        """
        # 8 digit transaction ID
        rng = random.randint(10000000,99999999)

        # ensure no duplicate IDs
        while rng in Transaction.txn_id_list:
            rng = random.randint(10000000,99999999)

        Transaction.txn_id_list.append(rng)

        return rng
    
    def genTxnType(self):
        """
            Generate a transation type from the listed banking methods(Card, Internet Banking, UPI, Wallet). Aiming for a trend of 50% digital wallet, 34% card (credit + debit), 7% upi (A2A), and the remaining 9% as internet banking (can be modified later if desired). 

            Trend data from:
            https://www.statista.com/statistics/1111233/payment-method-usage-transaction-volume-share-worldwide/

        """

        # float between 0 and 1
        rng = random.random()

        if rng <= 0.5:
            return 'wallet'
        elif rng > 0.5 and rng <= 0.84:
            return 'card'
        elif rng > 0.84 and rng <= 0.91:
            return 'upi'
        else:
            return 'internet banking'
    
    def genTxnSuccess(self):
        """
            Generate a flag to indicate transaction success or failure (0 or 1), current trend is 95% success rate (can be modified later if desired).
        """
        # float between 0 and 1
        rng = random.random()
        
        # 90% transaction success rate
        if rng <= 0.9:
            return 1
        else:
            return 0
        
    def genFailureReason(self):
        """
            Generate a failure reason- dependent on self.paymentTxnSuccess and self.paymentType .
        """
        
        if self.paymentTxnSuccess == 1:
            return 'payment successful'
        
        else:
            if self.paymentType == 'card':
                rng = random.randint(1,6)
                if rng == 1:
                    return 'insufficient credit'
                if rng == 2:
                    return 'insufficient funds'
                elif rng == 3:
                    return 'card expired'
                elif rng == 4:
                    return 'payment method not accepted at merchant'
                elif rng == 5:
                    return 'billing address incorrect'
                else:
                    return 'bank refused to authorize transaction'
                
            else:
                rng = random.randint(1,4)
                if rng == 1:
                    return 'insufficient funds'
                if rng == 2:
                    return 'insufficient credit'  
                elif rng == 3:
                    return 'payment method not accepted at merchant'
                else:
                    return 'bank refused to authorize transaction'
                  
    @property
    def paymentTxnId(self):
        return self.__paymentTxnId
    
    @property
    def paymentType(self):
        return self.__paymentType
    
    @property 
    def paymentTxnSuccess(self):
        return self.__paymentTxnSuccess
    
    @property
    def failureReason(self):
        return self.__failureReason
    
    # def get_dataframe(self):
        
    #     spark_session = SparkSingleton.getInstance()
    #     schema = StructType([
    #         StructField("payment_txn_id", IntegerType(), False),
    #         StructField("payment_type", StringType(), False),
    #         StructField("payment_txn_success", IntegerType(), False),
    #         StructField("failure_reason", StringType(), False)
    #     ])

    #     df = spark_session.createDataFrame([(self.paymentTxnId, self.paymentType, self.paymentTxnSuccess, self.failureReason)], schema=schema)
        
    #     return df
    
    # @property
    # def cardTrend(self):
    #     return self.__cardTrend    
    
    # @cardTrend.setter
    # def cardTrend(self, value: float):
    #     self.__cardTrend = value

    # @property 
    # def txnSuccessRate(self):
    #     return self.__txnSuccessRate
    
    # @txnSuccessRate.setter
    # def txnSuccessRate(self, value: float):
    #     self.__txnSuccessRate = value


# def main():
#     """
#         Create 1000 transaction dataframes.
#     """
#     main_df: DataFrame = None
#     for i in range(1000):
#         transaction = Transaction()
#         df = transaction.get_dataframe()
#         if main_df is None:
#             main_df = df
#         else:
#             main_df = main_df.union(df)
    
#     main_df.show(main_df.count(), truncate=False)

# if __name__ == "__main__":
#     main()