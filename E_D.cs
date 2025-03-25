using Dapper;
using Microsoft.Practices.EnterpriseLibrary.Data;
using NAV.Common.Logging;
using NAVBO.BusinessObjects.MDM;
using NAVCOMMONMODULES.NAVDAL.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace NAVDAL.MDM
{
    public class EntityDAL
    {
        Dictionary<string, object> param = new Dictionary<string, object>();
        private readonly ILogger publishLogger;
        private readonly ILogger sqlLogger;
        static ILogger logger = Log4NetService.Instance.GetLogger("NAVMDMPublish");

        public EntityDAL()
        {
            publishLogger = Log4NetService.Instance.GetLogger("NAVMDMPublish");
            sqlLogger = Log4NetService.Instance.GetLogger("SQL");
        }

        public List<EntityValidateInfo> GetValidateInfo(int entityID, int? brokerID, int entityTypeCode)
        {
            try
            {
                param = new Dictionary<string, object>();
                Database db = CommonDAL.CreateCommonDatabase();
                IDbConnection connection = new SqlConnection(db.ConnectionString);
                var parameters = new DynamicParameters();
                List<EntityValidateInfo> lstEntityValidateInfo = new List<EntityValidateInfo>();

                param.Add("Procedure", "ValidateEntityCreationFromMDM");
                param.Add("EntityID", entityID);
                param.Add("BrokerID", brokerID);
                param.Add("EntityTypeCode", entityTypeCode);
                param.Add("ConnectionString", db.ConnectionString);
                publishLogger.Info.Write("EntityDAL.GetEntityValidateInfo START:", param);
                sqlLogger.Info.Write("Proc: ValidateEntityCreationFromMDM START:", param);

                parameters.Add("@entityID", entityID, dbType: DbType.Int32);
                if (brokerID != null)
                    parameters.Add("@brokerID", brokerID, dbType: DbType.Int32);
                parameters.Add("@entityType", entityTypeCode, dbType: DbType.Int32);
                parameters.Add("@retunValue", dbType: DbType.Int32, direction: ParameterDirection.ReturnValue);

                using (var multipleResult = connection.QueryMultiple("ValidateEntityCreationFromMDM", parameters, commandType: CommandType.StoredProcedure))
                {
                    lstEntityValidateInfo = multipleResult.Read().Select(x => new EntityValidateInfo
                    {
                        ClientID = x.ClientID,
                        EntityID = x.EntityID,
                        EntityTypeCode = x.EntityType,
                        IsBalanceExist = x.IsBalanceExist,
                    }).ToList();

                    int returnResult = Convert.ToInt32(parameters.Get<int>("@retunValue"));
                    if (returnResult != 0)
                    {
                        publishLogger.Info.Write("EntityDAL.GetEntityValidateInfo END: Problem in ValidateEntityCreationFromMDM. " + returnResult, param);
                        sqlLogger.Info.Write("ValidateEntityCreationFromMDM END: Problem in ValidateEntityCreationFromMDM. " + returnResult, param);
                    }

                    return lstEntityValidateInfo;
                }
            }
            catch (SqlException ex)
            {
                publishLogger.Critical("Problem in EntityDAL.GetEntityValidateInfo.ValidateEntityCreationFromMDM proc ", param, ex);
                param.Add("Exception", ex.Message);
                throw;
            }
            finally
            {
                publishLogger.Info.Write("EntityDAL.GetEntityValidateInfo END: ", param);
                sqlLogger.Info.Write("ValidateEntityCreationFromMDM END:", param);
            }
        }

        public int ProcessEntity(bool isStaging, ProcessEntity objSaveEntity)
        {
            int isRelationshipModified = 0;
            try
            {
                param = new Dictionary<string, object>();
                Database db = CommonDAL.CreateCommonDatabase();
                IDbConnection connection = new SqlConnection(db.ConnectionString);

                param.Add("Procedure: ", "ProcessEntity");
                param.Add("isStaging", isStaging);
                param.Add("entityType", objSaveEntity.EntityType);
                param.Add("jEntityDetail", objSaveEntity.EntityDetail);
                param.Add("jEntityAttributes", objSaveEntity.EntityAttributes);
                param.Add("jEntityRelationships", objSaveEntity.EntityRelationshipsDetail);
                param.Add("jEntityTransactionalMapping", objSaveEntity.EntityTransactionalMapping);
                param.Add("jRelationshipsConstraints", objSaveEntity.RelationshipConstraint);
                param.Add("ConnectionString", db.ConnectionString);

                publishLogger.Info.Write("EntityDAL.ProcessEntity START: ", param);
                sqlLogger.Info.Write("Proc: ProcessEntity START:", param);

                var parameters = new DynamicParameters();
                parameters.Add("@isStaging", isStaging, DbType.Boolean);
                parameters.Add("@entityType", objSaveEntity.EntityType, DbType.Int32);
                parameters.Add("@jEntityDetail", !string.IsNullOrEmpty(objSaveEntity.EntityDetail) ? objSaveEntity.EntityDetail : null);
                parameters.Add("@jEntityAttributes", !string.IsNullOrEmpty(objSaveEntity.EntityAttributes) ? objSaveEntity.EntityAttributes : null);
                parameters.Add("@jEntityRelationships", !string.IsNullOrEmpty(objSaveEntity.EntityRelationshipsDetail) ? objSaveEntity.EntityRelationshipsDetail : null);
                parameters.Add("@jEntityTransactionalMapping", !string.IsNullOrEmpty(objSaveEntity.EntityTransactionalMapping) ? objSaveEntity.EntityTransactionalMapping : null);
                parameters.Add("@jRelationshipsConstraints", !string.IsNullOrEmpty(objSaveEntity.RelationshipConstraint) ? objSaveEntity.RelationshipConstraint : null);
                parameters.Add("@retunValue", dbType: DbType.Int32, direction: ParameterDirection.ReturnValue);
                parameters.Add("@isRelationshipModified", dbType: DbType.Int16, direction: ParameterDirection.Output);

                using (var multipleResult = connection.QueryMultiple("ProcessEntity", parameters, commandType: CommandType.StoredProcedure))
                {
                    int returnResult = Convert.ToInt32(parameters.Get<int>("@retunValue"));
                    isRelationshipModified = Convert.ToInt16(parameters.Get<Int16>("@isRelationshipModified"));
                    if (returnResult != 0)
                    {
                        publishLogger.Info.Write("There was some error in ProcessEntity. ReturnValue: " + returnResult, param);
                        sqlLogger.Info.Write("Proc: ProcessEntity: There was some error in ProcessEntity. ReturnValue: " + returnResult, param);
                    }
                }
            }
            catch (SqlException ex)
            {
                publishLogger.Critical("Problem in EntityDAL.ProcessEntity: ", param, ex);
                param.Add("Exception", ex.Message);
                throw;
            }
            finally
            {
                publishLogger.Info.Write("EntityDAL.ProcessEntity END: ", param);
                sqlLogger.Info.Write("Proc: ProcessEntity END: ", param);
            }
            return isRelationshipModified;
        }

        public int SaveRelationshipStatus(int clientId)
        {
            int id = 0;
            Database db = CommonDAL.CreateCommonDatabase();
            IDbConnection connection = new SqlConnection(db.ConnectionString);
            param = new Dictionary<string, object>();
            param.Add("InsertIntoRelationshipStatus.clientId", clientId);
            sqlLogger.Info.Write("Start IndirectRelationshipStatus insertion", param);
            try
            {
                StringBuilder query = new StringBuilder();

                query.Append(" Insert into IndirectEntityRelationshipStatus(ClientID, Status, VersionDate, VersionSource) ");
                query.Append(" values(" + clientId + ",0,'" + System.DateTime.Now + "','rabbitMQ: Process Entity') ");
                query.Append(" SELECT CAST(SCOPE_IDENTITY() as int) AutoGenID ");
                id = connection.Query<int>(query.ToString()).Single();

                sqlLogger.Info.Write("End IndirectRelationshipStatus insertion", param);
                return id;

            }
            catch (Exception ex)
            {
                sqlLogger.Critical("Problem in EntityDAL.InsertIntoRelationshipStatus", param, ex);
                throw;

            }
        }


        public void UpdateRelationshipStatus(int autoGenID)
        {
            string connectionString = string.Empty;
            Database db = CommonDAL.CreateCommonDatabase();
            connectionString = db.ConnectionString;
            IDbConnection connection = new SqlConnection(connectionString);
            param = new Dictionary<string, object>();
            param.Add("InsertIntoRelationshipStatus.autoGenID", autoGenID);
            sqlLogger.Info.Write("Start IndirectRelationshipStatus updation", param);
            try
            {
                string query = " UPDATE IndirectEntityRelationshipStatus SET Status = 1 WHERE AutoGenID = " + autoGenID;
                connection.Query(query);
                sqlLogger.Info.Write("End IndirectRelationshipStatus updation", param);

            }
            catch (Exception ex)
            {
                sqlLogger.Critical("Problem in EntityDAL.UpdateRelationshipStatus", param, ex);
                throw;

            }
        }

        #region Save Entity Data
        public static void ProcessEntity(ProcessEntity objSaveEntity, Guid BatchID, int clientId = 0)
        {
            try
            {
                logger.Info.Write(string.Format(" START:EntityDAL.ProcessEntity EntityDetailMsg={0} || EntityRelationshipsMsg={1} || EntityTransetionalMapMsg={2}  ", objSaveEntity.EntityDetail, objSaveEntity.EntityRelationshipsDetail, objSaveEntity.EntityTransactionalMapping));

                var connection = clientId == 0 ? new SqlConnection(CommonDAL.GetMasterConnectionString()) : new SqlConnection(CommonDAL.GetClientConnectionString(clientId));

                using (SqlConnection con = connection)
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("SyncEntityWithMDM", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jEntityDetail", SqlDbType.NVarChar).Value = objSaveEntity.EntityDetail;
                    cmd.Parameters.Add("@jEntityRelationships", SqlDbType.NVarChar).Value = objSaveEntity.EntityRelationshipsDetail;
                    cmd.Parameters.Add("@jEntityTransactionalMapping", SqlDbType.NVarChar).Value = objSaveEntity.EntityTransactionalMapping;
                    cmd.Parameters.Add("@jEntityAttributes", SqlDbType.NVarChar).Value = objSaveEntity.EntityAttributes;
                    cmd.Parameters.Add("@BatchID", SqlDbType.UniqueIdentifier).Value = BatchID;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in ProcessBrokers.");
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.ProcessEntity:EntityDetailMsg={0} || EntityRelationshipsMsg={1} || EntityTransetionalMapMsg={2}  ", objSaveEntity.EntityDetail, objSaveEntity.EntityRelationshipsDetail, objSaveEntity.EntityTransactionalMapping), ex);
                throw;
            }
            finally
            {
                logger.Info.Write(string.Format(" END:EntityDAL.ProcessEntity EntityDetailMsg={0} || EntityRelationshipsMsg={1} || EntityTransetionalMapMsg={2}  ", objSaveEntity.EntityDetail, objSaveEntity.EntityRelationshipsDetail, objSaveEntity.EntityTransactionalMapping));
            }
        }




        public static void ProcessEntityDecoupled(ProcessEntity objSaveEntity, string jsonSysInfo, int transactionalClientID,Guid BatchID, int clientId = 0)
        {
            try
            {
                logger.Info.Write(string.Format(" START:EntityDAL.ProcessEntityDecoupled EntityDetailMsg={0} || EntityRelationshipsMsg={1} || EntityTransetionalMapMsg={2}  ", objSaveEntity.EntityDetail, objSaveEntity.EntityRelationshipsDetail, objSaveEntity.EntityTransactionalMapping));

                var connection = clientId == 0 ? new SqlConnection(CommonDAL.GetMasterConnectionString()) : new SqlConnection(CommonDAL.GetClientConnectionString(clientId));

                using (SqlConnection con = connection)
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("SyncEntityWithMDMDecoupled", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jEntityDetail", SqlDbType.NVarChar).Value = objSaveEntity.EntityDetail;
                    cmd.Parameters.Add("@jEntityRelationships", SqlDbType.NVarChar).Value = objSaveEntity.EntityRelationshipsDetail;
                    cmd.Parameters.Add("@jEntityTransactionalMapping", SqlDbType.NVarChar).Value = objSaveEntity.EntityTransactionalMapping;
                    cmd.Parameters.Add("@jEntityAttributes", SqlDbType.NVarChar).Value = objSaveEntity.EntityAttributes;


                    cmd.Parameters.Add("@jsonSysInfo", SqlDbType.NVarChar).Value = jsonSysInfo;
                    cmd.Parameters.Add("@transactionalClientID", SqlDbType.NVarChar).Value = transactionalClientID;
                    cmd.Parameters.Add("@clientId", SqlDbType.NVarChar).Value = clientId;
                    cmd.Parameters.Add("@BatchID", SqlDbType.UniqueIdentifier).Value = BatchID;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();

                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in ProcessEntityDecoupled .");
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.ProcessEntityDecoupled:EntityDetailMsg={0} || EntityRelationshipsMsg={1} || EntityTransetionalMapMsg={2}  ", objSaveEntity.EntityDetail, objSaveEntity.EntityRelationshipsDetail, objSaveEntity.EntityTransactionalMapping), ex);
                throw;
            }
            finally
            {
                logger.Info.Write(string.Format(" END:EntityDAL.ProcessEntityDecoupled EntityDetailMsg={0} || EntityRelationshipsMsg={1} || EntityTransetionalMapMsg={2}  ", objSaveEntity.EntityDetail, objSaveEntity.EntityRelationshipsDetail, objSaveEntity.EntityTransactionalMapping));
            }
        }




        #endregion

        #region Save Broker Data
        public static void ProcessBrokers(string jSaveBrokers,Guid BatchID)
        {
            try
            {
                logger.Info.Write(string.Format("START:EntityDAL.ProcessBrokers MsgBody={0}  ", jSaveBrokers));
                using (SqlConnection con = new SqlConnection(CommonDAL.GetMasterConnectionString()))
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("SyncBrokersWithMDM", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jBrokersData", SqlDbType.NVarChar).Value = jSaveBrokers;
                    cmd.Parameters.Add("@BatchID", SqlDbType.UniqueIdentifier).Value = BatchID;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in ProcessBrokers.");
                    }
                }

            }
            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.ProcessBrokers MsgBody={0}  ", jSaveBrokers), ex);
            }
            finally
            {
                logger.Info.Write(string.Format("End:EntityDAL.ProcessBrokers MsgBody={0}  ", jSaveBrokers));
            }

        }
        #endregion

        #region Save Broker accounts
        public static void ProcessBrokerAccounts(string jSaveBrokersAcc, string jSaveEntityRelationship,Guid BatchID, int clientId = 0)
        {
            try
            {
                logger.Info.Write(string.Format(" START:EntityDAL.ProcessBrokers BrokerAccountMsg={0} || EntityRelationshipMsg={1}", jSaveBrokersAcc, jSaveEntityRelationship));

                var connection = clientId == 0 ? new SqlConnection(CommonDAL.GetMasterConnectionString()) : new SqlConnection(CommonDAL.GetClientConnectionString(clientId));

                using (SqlConnection con = connection)
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("SyncBrokerAccountsWithMDM", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jBrokerAccounts", SqlDbType.NVarChar).Value = jSaveBrokersAcc;
                    cmd.Parameters.Add("@jEntityRelationships", SqlDbType.NVarChar).Value = jSaveEntityRelationship;
                    cmd.Parameters.Add("@BatchID", SqlDbType.UniqueIdentifier).Value = BatchID;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in ProcessBrokerAccounts.");
                    }
                }
                logger.Info.Write(string.Format(" END:EntityDAL.ProcessBrokers BrokerAccountMsg={0} || EntityRelationshipMsg={1}", jSaveBrokersAcc, jSaveEntityRelationship));
            }
            catch (Exception ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.ProcessBrokerAccountsmsgBody={0} || EntityRelationshipMsg={1} ", jSaveBrokersAcc, jSaveEntityRelationship), ex);
                throw;
            }

        }


        public static void ProcessBrokerAccountsDecoupled(string jSaveBrokersAcc, string jSaveEntityRelationship, string jsonSysInfo, string jsonBrokerInfo, int transactionalClientID,Guid BatchID,
            int clientId = 0)
        {
            try
            {
                logger.Info.Write(string.Format(" START:EntityDAL.ProcessBrokerAccountsDecoupled BrokerAccountMsg={0} || EntityRelationshipMsg={1}", jSaveBrokersAcc, jSaveEntityRelationship));

                var connection =  new SqlConnection(CommonDAL.GetClientConnectionString(clientId));

                using (SqlConnection con = connection)
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("SyncBrokerAccountsWithMDMDecoupled", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jBrokerAccounts", SqlDbType.NVarChar).Value = jSaveBrokersAcc;
                    cmd.Parameters.Add("@jEntityRelationships", SqlDbType.NVarChar).Value = jSaveEntityRelationship;

                    cmd.Parameters.Add("@jsonSysInfo", SqlDbType.NVarChar).Value = jsonSysInfo;
                    cmd.Parameters.Add("@jsonBrokerInfo", SqlDbType.NVarChar).Value = jsonBrokerInfo;
                    cmd.Parameters.Add("@transactionalClientID", SqlDbType.NVarChar).Value = transactionalClientID;
                    cmd.Parameters.Add("@clientId", SqlDbType.NVarChar).Value = clientId;
                    cmd.Parameters.Add("@BatchID", SqlDbType.UniqueIdentifier).Value = BatchID;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in ProcessBrokerAccounts.");
                    }
                }
                logger.Info.Write(string.Format(" END:EntityDAL.ProcessBrokerAccountsDecoupled BrokerAccountMsg={0} || EntityRelationshipMsg={1}", jSaveBrokersAcc, jSaveEntityRelationship));
            }
            catch (Exception ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL. ProcessBrokerAccountsDecoupled BrokerAccountsmsgBody={0} || EntityRelationshipMsg={1} ", jSaveBrokersAcc, jSaveEntityRelationship), ex);
                throw;
            }

        }




        #endregion

        #region Save Reporting delivery and processing groups
        public static void ProcessReportingDeliveryAndProcessingGroups(string jSaveGrpProperties, string jSaveGrpMapping)
        {
            try
            {
                logger.Info.Write(string.Format(" START:EntityDAL.ProcessReportingDeliveryAndProcessingGroups GroupProperties={0} || GroupMapping={1}", jSaveGrpProperties, jSaveGrpMapping));

                using (SqlConnection con = new SqlConnection(CommonDAL.GetMasterConnectionString()))
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("SyncReportingDeliveryAndProcessingGroupsWithMDM", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jReportingDeliveryGroups", SqlDbType.NVarChar).Value = jSaveGrpProperties;
                    cmd.Parameters.Add("@jgroupEntityMappings", SqlDbType.NVarChar).Value = jSaveGrpMapping;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in ProcessReportingDeliveryAndProcessingGroups.");
                    }
                }
                logger.Info.Write(string.Format(" END:EntityDAL.ProcessReportingDeliveryAndProcessingGroups GroupProperties={0} || GroupMapping={1}", jSaveGrpProperties, jSaveGrpMapping));
            }
            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.ProcessReportingDeliveryAndProcessingGroups GroupProperties={0} || GroupMapping={1} ", jSaveGrpProperties, jSaveGrpMapping), ex);
            }

        }
        #endregion

        #region Save Business Client Relationships
        public static void ProcessBusinessClientRelationships(string jSaveBusinessClientRelationship)
        {
            try
            {
                logger.Info.Write(string.Format("START:EntityDAL.ProcessBusinessClientRelationships MsgBody={0}  ", jSaveBusinessClientRelationship));
                using (SqlConnection con = new SqlConnection(CommonDAL.GetMasterConnectionString()))
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("SyncBusinessClientRelationshipsWithMDM", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jBusinessClientRelationships", SqlDbType.NVarChar).Value = jSaveBusinessClientRelationship;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in ProcessBusinessClientRelationships.");
                    }
                }

            }
            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.ProcessBrokers MsgBody={0}  ", jSaveBusinessClientRelationship), ex);
            }
            finally
            {
                logger.Info.Write(string.Format("End:EntityDAL.ProcessBrokers MsgBody={0}  ", jSaveBusinessClientRelationship));
            }

        }
        #endregion

        #region Save Transection Clients
        public static void ProcessTransactionClients(string jSaveTransClients)
        {
            try
            {
                logger.Info.Write(string.Format("START:EntityDAL.ProcessTransactionClients MsgBody={0}  ", jSaveTransClients));
                using (SqlConnection con = new SqlConnection(CommonDAL.GetMasterConnectionString()))
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("SyncTransactionClientsWithMDM", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jTransactionalClients", SqlDbType.NVarChar).Value = jSaveTransClients;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in ProcessTransactionClients.");
                    }
                }

            }
            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.ProcessTransactionClients MsgBody={0}  ", jSaveTransClients), ex);
            }
            finally
            {
                logger.Info.Write(string.Format("End:EntityDAL.ProcessTransactionClients MsgBody={0}  ", jSaveTransClients));
            }

        }
        public static List<ClientDatabaseInfo> UiProcessTransactionClients(string jSaveTransClients)
        {
            List<ClientDatabaseInfo> clientDatabaseInfos = new List<ClientDatabaseInfo>();

            try
            {
                logger.Info.Write(string.Format("START:EntityDAL.UiProcessTransactionClients MsgBody={0}  ", jSaveTransClients));
                
                using (SqlConnection con = new SqlConnection(CommonDAL.GetMasterConnectionString()))
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("UiSyncTransactionClientsWithMDM", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jTransactionalClients", SqlDbType.NVarChar).Value = jSaveTransClients;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    using (IDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            clientDatabaseInfos.Add(new ClientDatabaseInfo()
                            {
                                ClientID = dr["ClientID"] == DBNull.Value ? 0 : Convert.ToInt32(dr["ClientID"]),
                                NewBusinessClientID = dr["NewBusinessClientID"] == DBNull.Value ? 0 : Convert.ToInt32(dr["NewBusinessClientID"]),
                                DatabaseName= Convert.ToString(dr["DatabaseName"]),
                                ServerName = Convert.ToString(dr["ServerName"]),

                            });
                        }
                    }
                  
                    if (clientDatabaseInfos.Count == 0)
                    {
                        logger.Info.Write("There is no clientDatabasesInfo in UiProcessTransactionClients.");
                    }
                    return clientDatabaseInfos;
                }

            }
            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.ProcessTransactionClients MsgBody={0}  ", jSaveTransClients), ex);
                return clientDatabaseInfos;
            }
            finally
            {
                logger.Info.Write(string.Format("End:EntityDAL.ProcessTransactionClients MsgBody={0}  ", jSaveTransClients));
            }

        }
        #endregion

        #region Save Account Attributes Data
        public static void ProcessAccountAttributes(string jSaveAccountAttributes,Guid BatchID ,int clientId = 0)
        {
            try
            {
                logger.Info.Write(string.Format("START:EntityDAL.ProcessAccountAttributes EntityAttributesMsg={0}", jSaveAccountAttributes));
                using (SqlConnection con = new SqlConnection(clientId == 0 ? CommonDAL.GetMasterConnectionString() : CommonDAL.GetClientConnectionString(clientId)))
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("SyncAccountAttributesWithMDM", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jAccountAttributes", SqlDbType.NVarChar).Value = jSaveAccountAttributes;
                    cmd.Parameters.Add("@BatchID", SqlDbType.UniqueIdentifier).Value = BatchID;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in ProcessAccountAttributes.");
                    }
                }

            }
            catch (Exception ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.ProcessAccountAttributes MsgBody={0}  ", jSaveAccountAttributes), ex);
                throw;
            }
            finally
            {
                logger.Info.Write(string.Format("End:EntityDAL.ProcessAccountAttributes MsgBody={0}  ", jSaveAccountAttributes));
            }

        }



        public static void ProcessAccountAttributesForDecoupled(string jSaveAccountAttributes, string jsonSysInfo, int businessClientID,Guid BatchID, int clientId = 0)
        {
            try
            {
                logger.Info.Write(string.Format("START:EntityDAL.ProcessAccountAttributesForDecoupled EntityAttributesMsg={0}", jSaveAccountAttributes));
                using (SqlConnection con = new SqlConnection(clientId == 0 ? CommonDAL.GetMasterConnectionString() : CommonDAL.GetClientConnectionString(clientId)))
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("SyncAccountAttributesWithMDMDecoupled", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jAccountAttributes", SqlDbType.NVarChar).Value = jSaveAccountAttributes;
                    cmd.Parameters.Add("@jsonSysInfo", SqlDbType.NVarChar).Value = jsonSysInfo;
                    cmd.Parameters.Add("@BusinessClientRelationshipID", SqlDbType.NVarChar).Value = businessClientID; 
                     cmd.Parameters.Add("@clientId", SqlDbType.NVarChar).Value = clientId;
                    cmd.Parameters.Add("@BatchID", SqlDbType.UniqueIdentifier).Value = BatchID;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in ProcessAccountAttributes.");
                    }
                }

            }
            catch (Exception ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.ProcessAccountAttributesForDecoupled MsgBody={0}  ", jSaveAccountAttributes), ex);
                throw;
            }
            finally
            {
                logger.Info.Write(string.Format("End:EntityDAL.ProcessAccountAttributesForDecoupled MsgBody={0}  ", jSaveAccountAttributes));
            }

        }


        public static EntityServiceMasterData GetMasterInfoForMDMEntityIntegrationService(DateTime fromDate, DateTime toDate, int clientID, string type)
        {
            try
            {
                Database db = CommonDAL.CreateCommonDatabase();
                DbCommand cmd = db.GetStoredProcCommand("UiGetMasterInfoForMDMEntityIntegrationService");

                db.AddInParameter(cmd, "@clientID", DbType.Int32, clientID);
                db.AddInParameter(cmd, "@fromDate", DbType.DateTime, fromDate);
                db.AddInParameter(cmd, "@toDate", DbType.DateTime, toDate);
                db.AddInParameter(cmd, "@type", DbType.String, type);



                Tuple<int, DateTime, int> tplItems = new Tuple<int, DateTime, int>(0, Convert.ToDateTime("01/01/1900"), 0);
                using (IDataReader dr = db.ExecuteReader(cmd))
                {
                    return GetEntityMasterDataCollection(dr, type);
                }

            }
            catch (Exception)
            {
                throw;
            }
        }




        private static EntityServiceMasterData GetEntityMasterDataCollection(IDataReader dataReader, string type)
        {
            EntityServiceMasterData entityMasterObj = new EntityServiceMasterData();
            entityMasterObj.lstEntitySysInfo = new List<EntitySysInfo>();
            entityMasterObj.lstBrokerMasterInfo = new List<BrokerMasterInfo>();

            EntitySysInfo entitySysInfoObj = null;
            BrokerMasterInfo brokerMasterInfoObj = null;
   
            while (dataReader.Read())
            {
                entitySysInfoObj = new EntitySysInfo();

                entitySysInfoObj.Type = dataReader["Type"] == DBNull.Value ? string.Empty : Convert.ToString(dataReader["Type"]);
                entitySysInfoObj.Code = dataReader["Code"] == DBNull.Value ? string.Empty : Convert.ToString(dataReader["Code"]);
                entitySysInfoObj.Name = dataReader["Name"] == DBNull.Value ? string.Empty : Convert.ToString(dataReader["Name"]);
                entitySysInfoObj.Value = dataReader["Value"] == DBNull.Value ? string.Empty : Convert.ToString(dataReader["Value"]);
              
               
                entityMasterObj.lstEntitySysInfo.Add(entitySysInfoObj);
            }

            if (type == "BrokerAccount")
            {
                dataReader.NextResult();

                while (dataReader.Read())
                {
                    brokerMasterInfoObj = new BrokerMasterInfo();
                    brokerMasterInfoObj.Type = dataReader["Type"] == DBNull.Value ? string.Empty : Convert.ToString(dataReader["Type"]);
                    brokerMasterInfoObj.BrokerID = dataReader["BrokerID"] == DBNull.Value ? -1 : Convert.ToInt32(dataReader["BrokerID"]);
                    brokerMasterInfoObj.GlobalBrokerID = dataReader["GlobalBrokerID"] == DBNull.Value ? -1 : Convert.ToInt32(dataReader["GlobalBrokerID"]);
                    brokerMasterInfoObj.ParentBrokerID = dataReader["ParentBrokerID"] == DBNull.Value ? -1 : Convert.ToInt32(dataReader["ParentBrokerID"]);
                    entityMasterObj.lstBrokerMasterInfo.Add(brokerMasterInfoObj);
                }
            }
            return entityMasterObj;
        }

        #endregion

        public static IEnumerable<Client> FetchClientsForAccounts(int accountId)
        {
            Database db = CommonDAL.CreateCommonDatabase();
            var dbCommand = db.GetSqlStringCommand("SELECT AM.client_id AS ClientId,CM.BusinessClientRelationshipID,AM.acc_id AS AccId FROM account_master AM INNER JOIN client_master CM ON AM.client_id = CM.client_id WHERE AM.MDMTopicEntityID = " + accountId);
            List<Client> clients = new List<Client>();

            try
            {
                using (IDataReader dr = db.ExecuteReader(dbCommand))
                {
                    while (dr.Read())
                    {
                        clients.Add(new Client()
                        {
                            ClientId = dr["ClientId"] == DBNull.Value ? 0 : Convert.ToInt32(dr["ClientId"]),
                            BusinessClientRelationshipID = dr["BusinessClientRelationshipID"] == DBNull.Value ? 0 : Convert.ToInt32(dr["BusinessClientRelationshipID"]),
                            AccountId = dr["AccId"] == DBNull.Value ? 0 : Convert.ToInt32(dr["AccId"])
                        });
                    }
                }

            }
            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.FetchClientsForAccounts AccountId={0}  ", accountId), ex);
            }

            return clients;
        }

        public static IEnumerable<Client> FetchClientsForEntities(string entityIds)
        {
            Database db = CommonDAL.CreateCommonDatabase();
            var dbCommand = db.GetSqlStringCommand($"SELECT ClientId=client_id FROM account_master with (nolock) where MDMTopicEntityID IN ({entityIds})");
            List<Client> clients = new List<Client>();

            try
            {
                using (IDataReader dr = db.ExecuteReader(dbCommand))
                {
                    while (dr.Read())
                    {
                        clients.Add(new Client()
                        {
                            ClientId = dr["ClientId"] == DBNull.Value ? 0 : Convert.ToInt32(dr["ClientId"]),
                        });
                    }
                }

            }
            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.FetchClientsForEntities entityIds={0}  ", entityIds), ex);
            }

            return clients;
        }

        public static void UiSaveHolidayForTransactionClientMailSend(string clientIDs)
        {
            try
            {
                logger.Info.Write(string.Format("START:EntityDAL.UiSaveHolidayForTransactionClientMailSend ClientIDs={0}", clientIDs));
                using (SqlConnection con = new SqlConnection(CommonDAL.GetMasterConnectionString()))
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("UiSaveHolidayForTransactionClientMailSend", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@cliendIDs", SqlDbType.NVarChar).Value = clientIDs;

                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in UiSaveHolidayForTransactionClientMailSend.");
                    }
                }
            }

            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.UiSaveHolidayForTransactionClientMailSend ClientIDs={0}", clientIDs), ex);
            }
            finally
            {
                logger.Info.Write(string.Format("End:EntityDAL.UiSaveHolidayForTransactionClientMailSend ClientIDs={0}", clientIDs));
            }
        }
        public static List<int> UiSaveHolidayForTransactionClient(int clientID,int businessClientID,string holidaysXML,string businessClientHolidaysXML)
        {
            List<int> listOfClients = new List<int>();

            try
            {
                logger.Info.Write(string.Format("START:EntityDAL.UiSaveHolidayForTransactionClient"));
                using (SqlConnection con = new SqlConnection(CommonDAL.GetClientConnectionString(clientID)))
                {
                    con.Open();

                    SqlCommand cmd = new SqlCommand("UiSaveHolidayForTransactionClientOnALLClients", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@holidaysXML", SqlDbType.NVarChar).Value = holidaysXML;
                    cmd.Parameters.Add("@businessClientHolidaysXML", SqlDbType.NVarChar).Value = businessClientHolidaysXML;
                    cmd.Parameters.Add("@businessClientID", SqlDbType.NVarChar).Value = businessClientID;
                    cmd.Parameters.Add("@clientID", SqlDbType.NVarChar).Value = clientID;

                    using (IDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            listOfClients.Add(dr["ClientID"] == DBNull.Value ? 0 : Convert.ToInt32(dr["ClientID"]));
                        }
                    }
                    if (listOfClients.Count()== 0)
                    {
                        logger.Info.Write("There is no client to publish Holiday.");
                    }
                    return listOfClients;
                }

            }
            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.UiSaveHolidayForTransactionClient ClientID : " +clientID), ex);
                return listOfClients;

            }
            finally
            {
                logger.Info.Write(string.Format("End:EntityDAL.UiSaveHolidayForTransactionClient ClientID:  " + clientID));
            }
        }
        public static IEnumerable<Client> FetchClientsForBusinessClients(string businessClientIds)
        {
            List<Client> clients = new List<Client>();
            try
            {
                using (SqlConnection conn = new SqlConnection(CommonDAL.GetMasterConnectionString()))
                {
                    SqlCommand cmd = new SqlCommand();
                    cmd.CommandType = CommandType.Text;
                    cmd.Connection = conn;
                    cmd.CommandText = string.Format(
                        "select A.client_id As ClientId,  BusinessClientRelationshipID from  client_master A with(nolock) where  A.status = 'E'  AND  A.BusinessClientRelationshipID IN (" + businessClientIds + ")");
                    conn.Open();

                    using (IDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            clients.Add(new Client()
                            {
                                ClientId = dr["ClientId"] == DBNull.Value ? 0 : Convert.ToInt32(dr["ClientId"]),
                                BusinessClientRelationshipID = dr["BusinessClientRelationshipID"] == DBNull.Value ? 0 : Convert.ToInt32(dr["BusinessClientRelationshipID"])
                            });
                        }
                    }

                }
            }
            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.FetchClientsForBusinessClients BusinessClientIds={0}  ", businessClientIds), ex);
            }

            return clients;
        }

        #region Save lock date
        public static void SaveFasLockDateData(string jSaveData, int clientId = 0)
        {
            try
            {
                logger.Info.Write(string.Format(" START:EntityDAL.SaveFasLockDateData jSaveData={0}", jSaveData));

                var connection = clientId == 0 ? new SqlConnection(CommonDAL.GetMasterConnectionString()) : new SqlConnection(CommonDAL.GetClientConnectionString(clientId));

                using (SqlConnection con = connection)
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand("SyncFasLockDateWithMDM", con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add("@jSaveData", SqlDbType.NVarChar).Value = jSaveData;
                    cmd.Parameters.Add("@retunValue", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
                    cmd.ExecuteNonQuery();
                    var i = Convert.ToInt16(cmd.Parameters["@retunValue"].Value);
                    if (i != 0)
                    {
                        logger.Info.Write("There was some error in update fas lock date.");
                    }
                }
                logger.Info.Write(string.Format(" End:EntityDAL.SaveFasLockDateData jSaveData={0}", jSaveData));

            }
            catch (SqlException ex)
            {
                logger.Critical(string.Format("Problem in EntityDAL.SaveFasLockDateData jSaveData={0}", jSaveData), ex);
                throw ex;
            }

        }
        #endregion
    }
}

