import pandas as pd
import cx_Oracle
import sqlalchemy
pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_colwidth', 100)
#parameters можно менять
HOSTNAME = 
SERVICE_NAME = 
USERNAME = '
PASSWORD = '
ENCODING = 
dsn_tsn = cx_Oracle.makedsn(HOSTNAME, 1521, service_name=SERVICE_NAME)
con = cx_Oracle.connect(USERNAME, PASSWORD, dsn_tsn, encoding=ENCODING)
dates = pd.read_excel("исходные данные.xlsx").astype(str)

today_year = dates.iloc[3, 0]
today_month = dates.iloc[2, 0]
previous_year = dates.iloc[1, 0]
month_of_previous_year = dates.iloc[0,0]

#запросы, можно менять, главное чтобы структуры таблиц не менялись
query_for_region = "select distinct cd_reg, lev, nm_reg, cd_reg_subject from uq.m$region where lev = 3 and segm = 'РА' and cntry = 'РФ' and cd_reg >= 1000000"
query_for_quantity_of_drug_stores = "select cd_reg_subject||'000' as cd_reg, count(*)  as A_QUANTITY from UQ.V$DRUGSTORE_ADDRESS_BY_SU_CLI dr, uq.m$region reg where dr.cd_reg=reg.cd_reg and reg.lev=3 group by cd_reg_subject"
query_for_GA = "SELECT NM_REG, sum(VOLSHT_IN), sum(VOLRUB_IN) FROM (SELECT\
                reg.nm_reg,\
                sd.stat_year,\
                sd.stat_month,\
                sd.sales_type_id,\
                SUM(volsht_in) as volsht_in,\
                SUM(volrub_in) as volrub_in\
                FROM\
                uq.stat_data\
                sd\
                INNER\
                JOIN\
                uq.m$region\
                reg\
                ON\
                sd.cd_reg = reg.cd_reg\
                WHERE((sd.stat_year = " + str(previous_year) + " and sd.stat_month >=" + str(month_of_previous_year) + ") or\
                      (sd.stat_year = " + str(today_year) + " and sd.stat_month <= " + str(today_month) + "))\
                AND\
                sd.sales_type_id in (3, 4, 23)\
                AND\
                reg.lev = 3\
                AND\
                reg.cntry = 'РФ'\
                GROUP\
                BY\
                reg.nm_reg,\
                sd.stat_year,\
                sd.sales_type_id,\
                sd.stat_month) GROUP BY NM_REG"
query_for_RA =           f"SELECT CASE\
                        WHEN sd.cd_reg = 1097000 THEN 1077000\
                        ELSE sd.cd_reg\
                     END\
                        AS cd_Reg,\
                     SUM (volrub_out) AS volrub_out,\
                     SUM (volsht_out) AS volsht_out\
                FROM uq.stat_data sd, uq.m$region reg\
               WHERE     sd.cd_REg = reg.cd_reg\
                     AND segm = 'РА'\
                     AND cntry = 'РФ'\
                     AND lev = 3\
                     AND sales_type_id = 9\
                     AND\
                     ((sd.stat_year = {str(previous_year)}\
and sd.stat_month >=  {str(month_of_previous_year) })\
                     OR\
                    (sd.stat_year =  {str(today_year)} and sd.stat_month <= {str(today_month) }))" \
          f"GROUP BY CASE\
                        WHEN sd.cd_reg = 1097000 THEN 1077000\
                        ELSE sd.cd_reg\
                     END"
query_for_CHAIN_DRUGSTORES = "select cd_regs, type, count(type) as quantity from\
(\
select cd_reg_subject | | '000' as cd_regs, drugstore_type as TYPE from uq.V$DRUGSTORE_ADDRESS_BY_SU_CLI inner join\
((select * from uq.m$region where lev = 3) reg)\
on uq.V$DRUGSTORE_ADDRESS_BY_SU_CLI.cd_reg = reg.cd_reg)\
group by cd_regs, type"
#функции
def putinora2(dfr:pd.DataFrame, usernm:str, passwd:str, tablenm:str) -> None:
    """"Процедура загрузки датафрейма в оракл чз sqlalchemy
    """
    dfr.columns = map(str.upper, dfr.columns)
    dfr.to_sql(tablenm.lower(),
               create_engine(f"oracle+cx_oracle://{usernm}:{passwd}@{cx_Oracle.makedsn('RA1DSM', 1521, 'dev14')}"),
               index=False,
               if_exists='append',
               dtype= { c: types.VARCHAR(dfr[c].str.len().max()) for c in dfr.columns[dfr.dtypes == 'object'].tolist() }
               )

def merge(TABLE1, TABLE2, left_on = "CD_REG", right_on = "CD_REG", how="left"):
    FLAG = 0
    if left_on == right_on:
        TABLE2 = TABLE2.rename({right_on: right_on + "_TMP"}, axis=1)
        right_on =  right_on+"_TMP"
        FLAG = 1
    TABLE1 = TABLE1.merge(right=TABLE2, left_on=left_on, right_on=right_on, how=how)
    if FLAG:
        TABLE1 = TABLE1[[c for c in TABLE1.columns if c != right_on]]
    return TABLE1

def count_chain_share(result, type, chain_drugstores):
    for i in result.index:
        if len(chain_drugstores[(chain_drugstores["CD_REGS"] == result.loc[i, "CD_REG"]) & (chain_drugstores["TYPE"] == type)]["QUANTITY"]) != 0:
            result.loc[i, type] = int(chain_drugstores[(chain_drugstores["CD_REGS"] == result.loc[i, "CD_REG"]) & (chain_drugstores["TYPE"] == type)]["QUANTITY"].iloc[0].replace("\xa0", "")) / int(result.loc[i, "A_QUANTITY"].replace("\xa0", ""))
        else:#
            result.loc[i, type] = 0
    return result


#ПОЛУЧЕНИЕ ДАННЫХ
REGION_CODE = pd.read_excel(dates.iloc[5, 0]).astype(str)
region = pd.read_sql(query_for_region, con=con).astype(str)
population = pd.read_excel(dates.iloc[7, 0], index_col=0).astype(str)
quantity_of_drug_stores = pd.read_sql(query_for_quantity_of_drug_stores, con=con).astype(str)
GA_money_and_packs = pd.read_sql(query_for_GA, con=con).astype(str)
# GA_money_and_packs.to_excel("GA.xlsx")
# GA_money_and_packs = pd.read_excel("GA.xlsx", index_col=0).astype(str)
RA_money_and_packs = pd.read_sql(query_for_RA, con=con).astype(str)
# RA_money_and_packs.to_excel("RA.xlsx")

#Подгружаю ЛПУ
LPU_BASE = pd.read_excel(dates.iloc[4, 0]).astype(str)
LPU_BASE = LPU_BASE[["Регион", "Юридическое наименование"]].groupby("Регион", as_index=False).count().astype(str)
LPU_BASE.rename({"Юридическое наименование":"LPU_QUANTITY"}, axis = 1, inplace=True)
reg_converter = pd.read_excel(dates.iloc[6, 0]).astype(str)
LPU_BASE = LPU_BASE.merge( reg_converter, left_on="Регион", right_on = "DB", how = "left")
LPU_BASE.drop(columns=["DB", "Регион"], inplace=True)
LPU_BASE.rename({"NOT_DB":"Регион"}, axis=1, inplace=True)
LPU_BASE.loc[LPU_BASE[LPU_BASE["Регион"] == "НЕНЕЦКИЙ АВТ. ОКРУГ"].index,"Регион"] = "НЕНЕЦКИЙ АВТОНОМНЫЙ ОКРУГ"
#
# RA_money_and_packs = pd.read_excel("RA.xlsx", index_col=0).astype(str)

chain_drugstores = pd.read_sql(query_for_CHAIN_DRUGSTORES, con=con).astype(str)
chain_drugstores["TYPE"] = chain_drugstores["TYPE"].map(lambda x: x.replace("малая локальная", "локальная"))
chain_drugstores["QUANTITY"] = chain_drugstores["QUANTITY"].astype(int)
chain_drugstores = chain_drugstores.groupby(["CD_REGS", "TYPE"],as_index = False).sum().astype(str)
result = pd.DataFrame(columns=["population"])




##Формируем таблицу
#привязываю население
result.loc[:, "population"] = population["population"]
result.loc[:, "reg_name"] = population["region_db"]
#Привязываю сиди_рег
result = result.merge(right=region, left_on="reg_name", right_on="NM_REG", how="left")
result.drop(columns=["NM_REG"], inplace=True)
result = merge(result, REGION_CODE)
#Привязываю кол-во аптек
result = merge(result, quantity_of_drug_stores)
#Привязываю кол-во ЛПУ
result = merge(result, LPU_BASE, left_on = "reg_name", right_on="Регион")
result.drop(columns=["Регион"], inplace=True)
#Привязываю money, packs RA
result = merge(result, RA_money_and_packs)
result = result.rename({"VOLRUB_OUT": "VOLRUB_OUT_RA", "VOLSHT_OUT": "VOLSHT_OUT_RA"}, axis=1)
#Привязываю money, packs GA
result = merge(result, GA_money_and_packs, left_on = "reg_name", right_on="NM_REG")
result.drop("NM_REG", axis = 1, inplace=True)
result = result.rename({"SUM(VOLRUB_IN)": "VOLRUB_OUT_GA", "SUM(VOLSHT_IN)": "VOLSHT_OUT_GA"}, axis=1)

#REGION_CODE
#создаю колонки
result.loc[:,["popul_p_store", "popul_p_lpu", "drug_cons_p_man_p_store_rub", \
              "drug_cons_p_man_p_LPU_rub", "drug_cons_p_man_p_store_pks", \
              "drug_cons_p_man_p_LPU_pks", "A_REVEN_per_store" ,"A_REVEN_p_LPU", \
              "A_MARKET_SHARE", "L_MARKET_SHARE", "A_PRICE_p_pk_RA", "A_PRICE_p_pk_GA", "A_CHAINS", "NON_CHAINS", "одиночные", "межрегиональная", \
              "региональная", "локальная", "федеральная"]] = "default"
#вычисляю население/аптека
result.loc[:,"popul_p_store"] = result["population"].map(lambda x: x.replace("\xa0","")).astype(int) / result["A_QUANTITY"].map(lambda x: x.replace("\xa0","")).astype(int)
#вычисляю население/ЛПУ
result.loc[:,"popul_p_lpu"] = result["population"].map(lambda x: x.replace("\xa0","")).astype(int) / result["LPU_QUANTITY"].map(lambda x: x.replace("\xa0","")).astype(int)
##вычисляю все остальное
result.loc[:,"drug_cons_p_man_p_store_rub"] = result["VOLRUB_OUT_RA"].map(lambda x: x.replace("\xa0","")).astype(float)  / result["population"].map(lambda x: x.replace("\xa0","")).astype(int) / result["A_QUANTITY"].map(lambda x: x.replace("\xa0","")).astype(int)
result.loc[:,"drug_cons_p_man_p_store_pks"] = result["VOLSHT_OUT_RA"].map(lambda x: x.replace("\xa0","")).astype(float)  / result["population"].map(lambda x: x.replace("\xa0","")).astype(int) / result["A_QUANTITY"].map(lambda x: x.replace("\xa0","")).astype(int)
result.loc[:,"drug_cons_p_man_p_LPU_rub"] = result["VOLRUB_OUT_GA"].map(lambda x: x.replace("\xa0","")).astype(float)  / result["population"].map(lambda x: x.replace("\xa0","")).astype(int) / result["LPU_QUANTITY"].map(lambda x: x.replace("\xa0","")).astype(int)
result.loc[:,"drug_cons_p_man_p_LPU_pks"] = result["VOLSHT_OUT_GA"].map(lambda x: x.replace("\xa0","")).astype(float)  / result["population"].map(lambda x: x.replace("\xa0","")).astype(int) / result["LPU_QUANTITY"].map(lambda x: x.replace("\xa0","")).astype(int)
result.loc[:,"A_REVEN_per_store"] = result["VOLRUB_OUT_RA"].map(lambda x: x.replace("\xa0","")).astype(float)  / result["A_QUANTITY"].map(lambda x: x.replace("\xa0","")).astype(int)
result.loc[:,"A_REVEN_p_LPU"] = result["VOLRUB_OUT_GA"].map(lambda x: x.replace("\xa0","")).astype(float)  / result["LPU_QUANTITY"].map(lambda x: x.replace("\xa0","")).astype(int)
result.loc[:,"A_MARKET_SHARE"] = result["VOLRUB_OUT_RA"].map(lambda x: x.replace("\xa0","")).astype(float)  / (result["VOLRUB_OUT_RA"].map(lambda x: x.replace("\xa0","")).astype(float) + result["VOLRUB_OUT_GA"].map(lambda x: x.replace("\xa0","")).astype(float))
result.loc[:,"L_MARKET_SHARE"] = result["VOLRUB_OUT_GA"].map(lambda x: x.replace("\xa0","")).astype(float)  / (result["VOLRUB_OUT_RA"].map(lambda x: x.replace("\xa0","")).astype(float) + result["VOLRUB_OUT_GA"].map(lambda x: x.replace("\xa0","")).astype(float))
result.loc[:,"A_PRICE_p_pk_RA"] = result["VOLRUB_OUT_RA"].map(lambda x: x.replace("\xa0","")).astype(float)  / result["VOLSHT_OUT_RA"].map(lambda x: x.replace("\xa0","")).astype(int)
result.loc[:,"A_PRICE_p_pk_GA"] = result["VOLRUB_OUT_GA"].map(lambda x: x.replace("\xa0","")).astype(float)  / result["VOLSHT_OUT_GA"].map(lambda x: x.replace("\xa0","")).astype(int)
result = count_chain_share(result, "одиночные", chain_drugstores)
result = count_chain_share(result, "межрегиональная", chain_drugstores)
result = count_chain_share(result, "региональная", chain_drugstores)
result = count_chain_share(result, "федеральная", chain_drugstores)
result = count_chain_share(result, "локальная", chain_drugstores)
result.loc[:,"NON_CHAINS"] = result["одиночные"]
result.loc[:,"A_CHAINS"] = result.loc[:,"NON_CHAINS"].map(lambda x: 1 - x)

result.rename({"A_MARKET_SHARE":"A_MARKETSHARE", "L_MARKET_SHARE":"L_MARKETSHARE", "NON_CHAINS":"A_NONCHAINS","LPU_QUANTITY":"L_QUANTITY", "popul_p_store":"A_PEOPLE_SIZE", "popul_p_lpu":"L_PEOPLE_SIZE", "drug_cons_p_man_p_store_rub":"A_CONSUMPTION_RUB", "drug_cons_p_man_p_LPU_rub":"L_CONSUMPTION_RUB", "drug_cons_p_man_p_store_pks":"A_CONSUMPTION_SHT", "drug_cons_p_man_p_LPU_pks":"L_CONSUMPTION_SHT", "A_REVEN_per_store":"A_REVENUE", "A_REVEN_p_LPU":"L_REVENUE", "A_PRICE_p_pk_RA":"A_PRICE_PACKAGE", "A_PRICE_p_pk_GA":"L_PRICE_PACKAGE", "федеральная":"A_FEDERAL_CHAINS", "межрегиональная":"A_INTERREGIONAL_CHAINS", "региональная":"A_REGIONAL_CHAINS", "локальная":"A_LOCAL_CHAINS"}, axis=1, inplace=True)
print(result.columns)
result_rus = result.copy()
result = result[['CD_REG','A_QUANTITY','L_QUANTITY','A_PEOPLE_SIZE','L_PEOPLE_SIZE','A_CONSUMPTION_RUB','L_CONSUMPTION_RUB','A_CONSUMPTION_SHT','L_CONSUMPTION_SHT','A_REVENUE','L_REVENUE','A_MARKETSHARE','L_MARKETSHARE','A_PRICE_PACKAGE','L_PRICE_PACKAGE','A_CHAINS','A_NONCHAINS','A_FEDERAL_CHAINS','A_INTERREGIONAL_CHAINS','A_REGIONAL_CHAINS','A_LOCAL_CHAINS','REGION_CODE']]
result = result.astype(str).applymap(lambda x: round(float(x), 2) if x.find('.')!=-1  else x)
#таблицы в ексель для базы
result.sort_values(by="CD_REG").to_excel("res.xlsx")


#меняю имена для клиентов
result_rus = result_rus[['A_QUANTITY','L_QUANTITY','A_PEOPLE_SIZE','L_PEOPLE_SIZE','A_CONSUMPTION_RUB','L_CONSUMPTION_RUB','A_CONSUMPTION_SHT','L_CONSUMPTION_SHT','A_REVENUE','L_REVENUE','A_MARKETSHARE','L_MARKETSHARE','A_PRICE_PACKAGE','L_PRICE_PACKAGE','A_CHAINS','A_NONCHAINS','A_FEDERAL_CHAINS','A_INTERREGIONAL_CHAINS','A_REGIONAL_CHAINS','A_LOCAL_CHAINS','reg_name']]
dict = {'A_QUANTITY':'Количество аптек','L_QUANTITY':'Количество ЛПУ','A_PEOPLE_SIZE':'Численность населения на 1 аптеку','L_PEOPLE_SIZE':'Численность населения на 1 ЛПУ','A_CONSUMPTION_RUB':'Потребление ЛС на душу на аптеку, руб','L_CONSUMPTION_RUB':'Потребление ЛС на душу на ЛПУ, руб','A_CONSUMPTION_SHT':'Потребление ЛС на душу на аптеку, упак','L_CONSUMPTION_SHT':'Потребление ЛС на душу на ЛПУ, упак','A_REVENUE':'Средняя выгручка на аптеку, руб','L_REVENUE':'Средняя выгручка на ЛПУ, руб','A_MARKETSHARE':'Доля коммерческого сегмента, %','L_MARKETSHARE':'Доля госпитального сегмента, %','A_PRICE_PACKAGE':'Средняя цена за упаковку, коммерческий сегмент','L_PRICE_PACKAGE':'Средняя цена за упаковку, госпитальный сегмент','A_CHAINS':'Доля сетевых аптек,%','A_NONCHAINS':'Доля не сетевых аптек,%','A_FEDERAL_CHAINS':'Доля федеральных сетей, %','A_INTERREGIONAL_CHAINS':'Доля межрегиональных сетей, %','A_REGIONAL_CHAINS':'Доля региональных сетей,%','A_LOCAL_CHAINS':'Доля локальных сетей,%','reg_name':'Регион'}
result_rus.rename(dict,  axis=1, inplace=True)
result_rus = result_rus.astype(str).applymap(lambda x: round(float(x), 2) if (x.find('.')!=-1 and x.replace(".", "").isdigit() == True)  else x)
#таблицы в ексель для клиентов других
result_rus.to_excel("res_rus.xlsx")
# putinora2(result, "uq", "tvy86-Eq45", "DRUGSTORE_PLUS_LPU_DESC")
# result.to_sql(name = "DRUGSTORE_PLUS_LPU_DESC", schema = "UQ", if_exists="replace", con=con)
# chain_drugstores.sort_values(by="CD_REGS").to_excel("chain.xlsx")


