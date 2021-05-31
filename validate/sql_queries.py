################
# TRANSACTIONS #
################

# Note: For comparisons between two tables the cols
# with numeric values have to be identical

# Validate DM Loeb, FactTrans
query_val_loeb_dm_FactTrans = """
SELECT
    LEFT(DateSK, 6) AS "yearmon",
    SUM(TotalValue) AS "total_value",
    COUNT(DISTINCT TrxID) AS "n_trx",
    COUNT(DISTINCT MemberSK) AS "n_members",
    MAX(CONVERT(DATE, CONVERT(VARCHAR(8), DateSK, 23))) AS "max_date",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM SnippLoyalty_DW_Loeb.dbo.FactTrans
WHERE DateSK BETWEEN 'start_date' AND 'end_date'
    AND MemberSK >= 0
    AND TransactionStatusSK = 2
    AND TransactionTypeSK IN (1, 2)
GROUP BY LEFT(DateSK, 6)
ORDER BY "yearmon";
"""

# Validate DM PKZ, FactTrans
query_val_pkz_dm_FactTrans = """
SELECT
    LEFT(DateSK, 6) AS "yearmon",
    SUM(TotalValue) AS "total_value",
    COUNT(DISTINCT TrxID) AS "n_trx",
    COUNT(DISTINCT MemberSK) AS "n_members",
    MAX(CONVERT(DATE, CONVERT(VARCHAR(8), DateSK, 23))) AS "max_date",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM SnippLoyalty_DW_PKZ.dbo.FactTrans
WHERE DateSK BETWEEN 'start_date' AND 'end_date'
    AND MemberSK >= 0
    AND TransactionStatusSK = 2
    AND TransactionTypeSK IN (1, 2)
GROUP BY LEFT(DateSK, 6)
ORDER BY "yearmon";
"""

# Validate DM Loeb, FactTransItem
query_val_loeb_dm_FactTransItem = """
SELECT
    LEFT(DateSK, 6) AS "yearmon",
    SUM(Amount) AS "total_value",
    COUNT(DISTINCT TrxID) AS "n_trx",
    COUNT(DISTINCT MemberSK) AS "n_members",
    MAX(CONVERT(DATE, CONVERT(VARCHAR(8), DateSK, 23))) AS "max_date",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM SnippLoyalty_DW_Loeb.dbo.FactTransItem
WHERE DateSK BETWEEN 'start_date' AND 'end_date'
    AND MemberSK >= 0
    AND TransactionStatusSK = 2
    AND TransactionTypeSK IN (1, 2)
GROUP BY LEFT(DateSK, 6)
ORDER BY "yearmon";
"""

# Validate DM PKZ, FactTransItem
query_val_pkz_dm_FactTransItem = """
SELECT
    LEFT(DateSK, 6) AS "yearmon",
    SUM(Amount) AS "total_value",
    COUNT(DISTINCT TrxID) AS "n_trx",
    COUNT(DISTINCT MemberSK) AS "n_members",
    MAX(CONVERT(DATE, CONVERT(VARCHAR(8), DateSK, 23))) AS "max_date",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM SnippLoyalty_DW_PKZ.dbo.FactTransItem
WHERE DateSK BETWEEN 'start_date' AND 'end_date'
    AND MemberSK >= 0
    AND TransactionStatusSK = 2
    AND TransactionTypeSK IN (1, 2)
GROUP BY LEFT(DateSK, 6)
ORDER BY "yearmon";
"""


# Validate bcl Loeb, EtlTransaction
query_val_loeb_bcl_EtlTransaction = """
SELECT
    LEFT(CONVERT(VARCHAR, TrxDate, 112), 6) AS "yearmon",
    SUM(TotalValue) AS "total_value",
    COUNT(DISTINCT TrxId) AS "n_trx",
    COUNT(DISTINCT UserId) AS "n_members",
    MAX(CONVERT(DATE, TrxDate)) AS "max_date",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM bcl_loeb.dbo.EtlTransaction
WHERE TrxDate BETWEEN 'start_date' AND 'end_date'
    AND	UserId >= 0
    AND	TrxStatusTypeId = 2
    AND	trxTypeid IN (1, 2)
GROUP BY LEFT(CONVERT(VARCHAR, TrxDate, 112), 6)
ORDER BY yearmon;
"""

# Validate bcl PKZ, EtlTransaction
query_val_pkz_bcl_EtlTransaction = """
SELECT
    LEFT(CONVERT(VARCHAR, TrxDate, 112), 6) AS "yearmon",
    SUM(TotalValue) AS "total_value",
    COUNT(DISTINCT TrxId) AS "n_trx",
    COUNT(DISTINCT UserId) AS "n_members",
    MAX(CONVERT(DATE, TrxDate)) AS "max_date",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM bcl_pkz.dbo.EtlTransaction
WHERE TrxDate BETWEEN 'start_date' AND 'end_date'
    AND	UserId >= 0
    AND	TrxStatusTypeId = 2
    AND	trxTypeid IN (1, 2)
GROUP BY LEFT(CONVERT(VARCHAR, TrxDate, 112), 6)
ORDER BY yearmon;
"""


###########
# MEMBERS #
###########

# DimMember check, DM Loeb
query_val_loeb_dm_DimMember = """
declare @default_date date = CONVERT(DATE, '1900-01-01')

SELECT
    MIN(MemberAK) AS "AK_lowest",
    MAX(MemberAK) AS "AK_highest",
    COUNT(DISTINCT MemberAK) AS "n_MemberAK",
    COUNT(CASE WHEN CreateDate = @default_date THEN MemberAK END) AS "n_dates_1Jan1900",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM SnippLoyalty_DW_LOEB.dbo.DimMember
WHERE MemberAK > 0;
"""

# DimMember check, DM Loeb
query_val_pkz_dm_DimMember = """
declare @default_date date = CONVERT(DATE, '1900-01-01')

SELECT
    MIN(MemberAK) AS "AK_lowest",
    MAX(MemberAK) AS "AK_highest",
    COUNT(DISTINCT MemberAK) AS "n_MemberAK",
    COUNT(CASE WHEN CreateDate = @default_date THEN MemberAK END) AS "n_dates_1Jan1900",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM SnippLoyalty_DW_PKZ.dbo.DimMember
WHERE MemberAK > 0;
"""

# 3 Random Customers (AKs), DM Loeb
query_val_loeb_dm_members = """
SELECT
    dm.MemberAK AS "member_AK",
    MAX(CONVERT(DATE, dm.CreateDate, 23)) AS "create_date",
    SUM(ft.TotalValue) AS "total_value_19",
    COUNT(DISTINCT ft.TrxID) AS "n_trx_19",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM SnippLoyalty_DW_LOEB.dbo.DimMember AS dm
JOIN SnippLoyalty_DW_LOEB.dbo.FactTrans AS ft
    ON dm.MemberSK = ft.MemberSK
WHERE dm.MemberAK IN (1217116, 1454182, 1812069)
    AND ft.DateSK BETWEEN 20190101 AND 20191231
GROUP BY dm.MemberAK
ORDER BY "member_AK";
"""

# 3 Random Customers (AKs), DM PKZ
query_val_pkz_dm_members = """
SELECT
    dm.MemberAK AS "member_AK",
    MAX(CONVERT(DATE, dm.CreateDate, 23)) AS "create_date",
    SUM(ft.TotalValue) AS "total_value_19",
    COUNT(DISTINCT ft.TrxID) AS "n_trx_19",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM SnippLoyalty_DW_PKZ.dbo.FactTrans AS ft
JOIN SnippLoyalty_DW_PKZ.dbo.DimMember AS dm
    ON dm.MemberSK = ft.MemberSK
WHERE dm.MemberAK IN (864669, 1088855, 1750360)
    AND ft.DateSK BETWEEN 20190101 AND 20191231
GROUP BY dm.MemberAK
ORDER BY dm.MemberAK;
"""


############
# PRODUCTS #
############

# 3 Random Products (AKs), DM Loeb - join on DimProd
query_val_loeb_dm_products = """
SELECT
    dti.TransactionItemAK AS "transaction_item_AK",
    SUM(fti.Amount) AS "total_value_19",
    COUNT(DISTINCT fti.TrxID) AS "n_trx_19",
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM SnippLoyalty_DW_LOEB.dbo.DimTransactionItem AS dti
JOIN SnippLoyalty_DW_LOEB.dbo.FactTransItem AS fti
    ON dti.TransactionItemSK = fti.TransactionItemSK
WHERE dti.TransactionItemAK IN (59742179, 64335210, 64767450)
    AND fti.DateSK BETWEEN 20190101 AND 20191231
GROUP BY dti.TransactionItemAK
ORDER BY transaction_item_AK;
"""

# 3 Random Products (TICode), DM PKZ - join on DTI
query_val_pkz_dm_products = """
WITH trx_2019 AS (
SELECT
    dti.TransactionItemCode,
    CASE WHEN fti.DateSK > 20190210 THEN dti.AnalysisCode8
         ELSE dti.AnalysisCode6 END AS 'AnalysisCode8',
    CASE WHEN fti.DateSK > 20190210 THEN dti.AnalysisCode6
         ELSE dti.AnalysisCode8 END AS 'AnalysisCode6',
       AnalysisCode10,
       AnalysisCode13,
    fti.Quantity,
    fti.Amount
FROM SnippLoyalty_DW_PKZ.dbo.FactTransItem AS fti
   JOIN SnippLoyalty_DW_PKZ.dbo.DimTransactionItem AS dti
    ON dti.TransactionItemSK = fti. TransactionItemSK
WHERE fti.TransactionStatusSK = 2
    AND fti.TransactionTypeSK IN (1)
    AND fti.DateSK BETWEEN 20190101 AND 20191231
    AND dti.TransactionItemCode in ('04240169996', '17207270743', '00506038507')
)

SELECT
    TransactionItemCode,
    AnalysisCode8,
    AnalysisCode6,
    AnalysisCode10,
    AnalysisCode13,
    SUM(Quantity) AS n_items,
    SUM(Amount) AS sum_amount,
    CONVERT(DATE, CURRENT_TIMESTAMP, 23) AS "date_db_check"
FROM trx_2019
GROUP BY TransactionItemCode,
         AnalysisCode6,
         AnalysisCode8,
         AnalysisCode10,
         AnalysisCode13
ORDER BY TransactionItemCode;
"""


# SPECIAL PRODUCT-QUERIES PKZ

query_val_pkz_duplicate_TISK = """
WITH dup_TISK AS (
    SELECT
       TransactionItemSK,
       COUNT(*) AS n_dup_TransactionItemSK
    FROM SnippLoyalty_DW_PKZ.dbo.FactTransItem
    WHERE TransactionItemSK > 0
    GROUP BY TransactionItemSK
    HAVING COUNT(*) > 1
)

SELECT ISNULL(
    (SELECT SUM(n_dup_TransactionItemSK) FROM dup_TISK)
    - (SELECT COUNT(TransactionItemSK) FROM dup_TISK), 0
) AS dup_TISK_count
"""


query_dict = {
    "loeb_bcl_EtlTransaction": query_val_loeb_bcl_EtlTransaction,
    "loeb_DM_FactTrans": query_val_loeb_dm_FactTrans,
    "loeb_DM_FactTransItem": query_val_loeb_dm_FactTransItem,
    "loeb_DM_DimMember_AK": query_val_loeb_dm_DimMember,
    "loeb_DM_three_members": query_val_loeb_dm_members,
    "loeb_DM_three_products": query_val_loeb_dm_products,
    "pkz_bcl_EtlTransaction": query_val_pkz_bcl_EtlTransaction,
    "pkz_DM_FactTrans": query_val_pkz_dm_FactTrans,
    "pkz_DM_FactTransItem": query_val_pkz_dm_FactTransItem,
    "pkz_DM_DimMember_AK": query_val_pkz_dm_DimMember,
    "pkz_DM_three_members": query_val_pkz_dm_members,
    "pkz_DM_three_products": query_val_pkz_dm_products,
    "pkz_DM_duplicate_TISK": query_val_pkz_duplicate_TISK,
}
