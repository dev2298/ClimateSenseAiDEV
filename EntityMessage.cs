using NavCommonModuleBLL.Common;
using System;
using NAVBO.BusinessObjects.MDM;
using Newtonsoft.Json;
using NAVDAL.MDM;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using NAV.MasterServiceClient;
using NAVCOMMONMODULES.NAVBO;

namespace NavCommonModuleBLL.MDM.Main2
{
    public class EntityMessage : Entity
    {
        private static object _entityMessageLock = new object();
        const string DATABASENOTFOUNDERRORMESSAGE = "DATABASE FOR THE CLIENT NOT FOUND";
        public EntityMessage() : base(RabbitMQConfiguration.EntityMessageQueueName)
        {
            primaryOperationId = Guid.NewGuid();
        }
        public override void Publish(string msgBody)
        {
            var operationId = Guid.NewGuid();
            lock (_entityMessageLock)
            {
                try
                {
                    logger.Info.Write(string.Format("START:EntityMessage.Publish  QueueName={0};MsgBody={1}", queueName, msgBody));
                    var startDateTime = DateTime.UtcNow;
                    PublishEntity entityData = JsonConvert.DeserializeObject<PublishEntity>(msgBody);
                    if (entityData != null)
                    {
                        try
                        {
                           if (entityData.EntityRelationships.Where(x => x.EntityID == 0).Any())
                            {
                                entityData.EntityRelationships.Where(x => x.EntityID == 0).Select(y => y).ToList().ForEach(z => z.EntityID = entityData.EntityID);
                            }
                            var objSaveEntity = new ProcessEntity
                            {
                                EntityDetail = JsonConvert.SerializeObject(entityData.EntityDetails),
                                EntityRelationshipsDetail = JsonConvert.SerializeObject(entityData.EntityRelationships),
                                EntityTransactionalMapping = JsonConvert.SerializeObject(entityData.EntityTransactionalMapping),
                                EntityAttributes = JsonConvert.SerializeObject(entityData.EntityAttributes)
                            };
                            var serializedcustomDetails = JsonConvert.SerializeObject(objSaveEntity);
                            _klogger.LogTraceMessage(0, primaryOperationId, operationId, 0, new DateTime(1900, 1, 1), "Before Save: Master EntityMessage for account fund mapping", serializedcustomDetails, startDateTime, DateTime.UtcNow, APPLICATIONNAME, Environment.CurrentDirectory, logger);

                            EntityDAL.ProcessEntity(objSaveEntity,primaryOperationId);
                            _klogger.LogTraceMessage(0, primaryOperationId, operationId, 0, new DateTime(1900, 1, 1), "After Save: Master EntityMessage for account fund mapping", serializedcustomDetails, startDateTime, DateTime.UtcNow, APPLICATIONNAME, Environment.CurrentDirectory, logger);

                        }
                        catch (Exception ex)
                        {
                            logger.Critical(string.Format("Problem in EntityMessage.Publish QueueName={0};MsgBody={1}", queueName, msgBody), ex);
                            _klogger.LogErrorMessage(0, ex.ToString(), String.Format("Problem in Master EntityMessage.Publish QueueName={0};MsgBody={1}", queueName, msgBody), primaryOperationId, operationId, new DateTime(1900, 1, 1), 1, 0, APPLICATIONNAME, Environment.CurrentDirectory, logger);
                            //string message = SendTeamsNotifications.GetNotificationCardForError("Account Entities and RelationShip MDM", ex.Message, "Master", operationId);
                            //SendTeamsNotifications.SendInfoToMsTeams(message, AppConfiguration.MessageSendServiceAPI, AppConfiguration.TeamsChannelURL);
                            throw;
                        }

                        SaveClientEntitydata(entityData, primaryOperationId);
                    }
                }
                catch (Exception ex)
                {
                    logger.Critical(string.Format("Problem in EntityMessage.Publish QueueName={0};MsgBody={1}", queueName, msgBody), ex);
                    _klogger.LogErrorMessage(0, ex.ToString(), String.Format("Problem in EntityMessage.Publish QueueName={0};MsgBody={1}", queueName, msgBody), primaryOperationId, operationId, new DateTime(1900, 1, 1), 1, 0, APPLICATIONNAME, Environment.CurrentDirectory, logger);
                }
                finally
                {
                    logger.Info.Write(string.Format("END:EntityMessage.Publish  QueueName={0};MsgBody={1}", queueName, msgBody));
                }
            }
        }

        private void SaveClientEntitydata(PublishEntity entityData,Guid BatchID)
        {
            var startDateTime = DateTime.UtcNow;
            var busniessClientRelationshipIds = string.Join(",", entityData.EntityDetails.Select(s => s.BusinessClientRelationshipID).Distinct());
            var clientdata = EntityDAL.FetchClientsForBusinessClients(busniessClientRelationshipIds);
            var clienIdsDict = clientdata.GroupBy(g => g.ClientId).ToDictionary(d => d.Key, d => d.ToList().Select(s => s.BusinessClientRelationshipID));
            var clienIds = clientdata.Select(s => s.ClientId).Where(x => x > 0);

            if (clienIds.Any())
            {
                EntityConfigClient entityConfig = new EntityConfigClient(AppConfiguration.Main2MastersAPI);
                var clientIds = string.Join(",", clienIds);
                ClientServiceClient clientConfig = new ClientServiceClient(AppConfiguration.Main2MastersAPI);
                Dictionary<int, bool> ismasterDecouplingApplicable = entityConfig.IsDecoupledMasterApplicableForClients(clientIds);
                var clientData = clientConfig.GetClientMasterData(clientIds);

                Parallel.ForEach(clienIds, (clientId) =>
                {
                    if (!ismasterDecouplingApplicable.ContainsKey(clientId) || !ismasterDecouplingApplicable[clientId])
                    {
                        var entityDeatils = entityData.EntityDetails.Where(w => clienIdsDict[clientId].ToList().Contains(w.BusinessClientRelationshipID));
                        var hsEntity = new HashSet<int>(entityDeatils.Select(s => s.EntityID));
                        var entityRel = entityData.EntityRelationships.Where(w => hsEntity.Contains(w.SourceEntityID) || hsEntity.Contains(w.TargetEntityID));
                        var entityAttributes = entityData.EntityAttributes.Where(w => hsEntity.Contains(w.EntityID));
                        var entityTransactionalMapping = entityData.EntityTransactionalMapping.Where(w => hsEntity.Contains(w.EntityID));
                        try
                        {
                            var objSaveEntity = new ProcessEntity
                            {
                                EntityDetail = JsonConvert.SerializeObject(entityDeatils),
                                EntityRelationshipsDetail = JsonConvert.SerializeObject(entityRel),
                                EntityTransactionalMapping = JsonConvert.SerializeObject(entityTransactionalMapping),
                                EntityAttributes = JsonConvert.SerializeObject(entityAttributes)
                            };

                            var serializedcustomDetails = JsonConvert.SerializeObject(objSaveEntity);
                            _klogger.LogTraceMessage(clientId, primaryOperationId, operationId, 0, new DateTime(1900, 1, 1), "Before Save: Client EntityMessage for account fund mapping", serializedcustomDetails, startDateTime, DateTime.UtcNow, APPLICATIONNAME, Environment.CurrentDirectory, logger);

                            EntityDAL.ProcessEntity(objSaveEntity, BatchID, clientId);
                            _klogger.LogTraceMessage(clientId, primaryOperationId, operationId, 0, new DateTime(1900, 1, 1), "After Save: Client EntityMessage for account fund mapping", serializedcustomDetails, startDateTime, DateTime.UtcNow, APPLICATIONNAME, Environment.CurrentDirectory, logger);

                        }
                        catch (Exception ex)
                        {
                            _klogger.LogErrorMessage(clientId, ex.ToString(), string.Format("Problem in Client Entity Message for Non Decoupled.Publish QueueName={0};MsgBody={1}", queueName, JsonConvert.SerializeObject(entityData)), primaryOperationId, operationId, new DateTime(1900, 1, 1), 1, 0, APPLICATIONNAME, Environment.CurrentDirectory, logger);
                            if (!ex.Message.ToUpperInvariant().Contains(DATABASENOTFOUNDERRORMESSAGE))
                            {
                                string message = SendTeamsNotifications.GetNotificationCardForError("Account Entities and RelationShip MDM Non Decoupled", ex.Message, "Client", operationId);
                                SendTeamsNotifications.SendInfoToMsTeams(message, AppConfiguration.MessageSendServiceAPI, AppConfiguration.TeamsChannelURL);

                            }
                        }
                    }
                    else
                    {
                        var client = (clientData != null && clientData.Count() > 0) ? clientData.Where(x => x.ClientID == clientId).FirstOrDefault() : null;

                        if (client != null)
                        {
                            var databaseName = (client.DataBaseName ?? "");
                            DateTime archUptoDate = (client.ArchUptoDate ?? Convert.ToDateTime("1/1/1900")); //@ArchivalDate
                            string clientName = (client.ClientName ?? "");
                            string brokerName = (client.BrokerName ?? "");
                            int brokerID = client.BrokerId;
                            string clientType = client.ClientType;
                            int businessClientRelationshipID = client.BusinessClientRelationshipID;
                            int transactionalClientID = client.MDMTransactionalClientID;



                            EntityServiceMasterData entityMasterObj = EntityDAL.GetMasterInfoForMDMEntityIntegrationService(Convert.ToDateTime("01/01/1900"), Convert.ToDateTime("12/31/9998"),
                                clientId, "Entity");


                            string jsonSysInfo = string.Empty;

                            if (entityMasterObj != null && entityMasterObj.lstEntitySysInfo != null && entityMasterObj.lstEntitySysInfo.Count() > 0)
                            {
                                jsonSysInfo = JsonConvert.SerializeObject(entityMasterObj.lstEntitySysInfo);
                            }
                            else
                            {
                                _klogger.LogErrorMessage(clientId, "Error in Master Data fetch for Decoupled client, Entity Message Save on Client DB",
                                string.Format("Problem in EntityMessage.Publish QueueName={0};MsgBody={1}", queueName, JsonConvert.SerializeObject(entityData.EntityDetails.Where(w => clienIdsDict[clientId].ToList().Contains(w.BusinessClientRelationshipID)))),
                                primaryOperationId, operationId, new DateTime(1900, 1, 1), 1, 0, APPLICATIONNAME, Environment.CurrentDirectory, logger);

                                throw new Exception("Error in Master Data fetch for Decoupled client, Entity Message Save on Client DB");
                            }



                            var entityDeatils = entityData.EntityDetails.Where(w => clienIdsDict[clientId].ToList().Contains(w.BusinessClientRelationshipID));
                            var hsEntity = new HashSet<int>(entityDeatils.Select(s => s.EntityID));
                            var entityRel = entityData.EntityRelationships.Where(w => hsEntity.Contains(w.SourceEntityID) || hsEntity.Contains(w.TargetEntityID));
                            var entityAttributes = entityData.EntityAttributes.Where(w => hsEntity.Contains(w.EntityID));
                            var entityTransactionalMapping = entityData.EntityTransactionalMapping.Where(w => hsEntity.Contains(w.EntityID));
                            try
                            {
                                var objSaveEntity = new ProcessEntity
                                {
                                    EntityDetail = JsonConvert.SerializeObject(entityDeatils),
                                    EntityRelationshipsDetail = JsonConvert.SerializeObject(entityRel),
                                    EntityTransactionalMapping = JsonConvert.SerializeObject(entityTransactionalMapping),
                                    EntityAttributes = JsonConvert.SerializeObject(entityAttributes)
                                };

                                var serializedcustomDetails = JsonConvert.SerializeObject(objSaveEntity);
                                _klogger.LogTraceMessage(clientId, primaryOperationId, operationId, 0, new DateTime(1900, 1, 1), "Before Save Decoupled: Client EntityMessage for account fund mapping", serializedcustomDetails, startDateTime, DateTime.UtcNow, APPLICATIONNAME, Environment.CurrentDirectory, logger);

                                EntityDAL.ProcessEntityDecoupled(objSaveEntity, jsonSysInfo, transactionalClientID, clientId);

                                _klogger.LogTraceMessage(clientId, primaryOperationId, operationId, 0, new DateTime(1900, 1, 1), "After Save Decoupled: Client EntityMessage for account fund mapping", serializedcustomDetails, startDateTime, DateTime.UtcNow, APPLICATIONNAME, Environment.CurrentDirectory, logger);

                            }
                            catch (Exception ex)
                            {
                                _klogger.LogErrorMessage(clientId, ex.ToString(), string.Format("Problem in Client Entity Message for Decoupled.Publish QueueName={0};MsgBody={1}", queueName, JsonConvert.SerializeObject(entityData)), primaryOperationId, operationId, new DateTime(1900, 1, 1), 1, 0, APPLICATIONNAME, Environment.CurrentDirectory, logger);
                                if (!ex.Message.ToUpperInvariant().Contains(DATABASENOTFOUNDERRORMESSAGE))
                                {
                                    string message = SendTeamsNotifications.GetNotificationCardForError("Account Entities and RelationShip MDM Decoupled", ex.Message, "Client", operationId);
                                    SendTeamsNotifications.SendInfoToMsTeams(message, AppConfiguration.MessageSendServiceAPI, AppConfiguration.TeamsChannelURL);
                                }
                            }

                        }
                        else
                        {
                            throw new Exception(string.Format("Error in Entity Message Publish save client entity data(decoupled), No client data found for clientId: {0}.", clientId));
                        }

                    }
                });
            }
            else
            {

                throw new Exception(string.Format("Error in Entity Message Publish save client entity data, No client found for businessClientRelationshipIds: {0}.", busniessClientRelationshipIds));
            }
        }
    }

}
