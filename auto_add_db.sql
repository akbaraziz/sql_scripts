/**

 * AlwaysOn Self-Population Script

 * By: Steve Gray / steve@mostlyharmful.net

 **/

USE [master]

GO

IF EXISTS (SELECT * FROM sys.tables WHERE name='hadr_pending_replicate')

	BEGIN

		DROP TABLE hadr_pending_replicate;

	END;

GO

CREATE TABLE hadr_pending_replicate (database_name VARCHAR(512) PRIMARY KEY NOT NULL, availability_group_name VARCHAR(2048) NOT NULL);

GO

IF EXISTS (SELECT * FROM sys.server_triggers WHERE name='ddl_hadr_autoreplicate')

	BEGIN

		DROP TRIGGER ddl_hadr_autoreplicate ON ALL SERVER;

	END;

GO

CREATE TRIGGER ddl_hadr_autoreplicate ON ALL SERVER

	FOR CREATE_DATABASE

AS

BEGIN

	SET NOCOUNT ON;

	DECLARE @DatabaseName NVARCHAR(2048)

	-- Find the availability group, if the CREATE DATABASE is occuring on an AG listener.

	DECLARE @AddToGroupName			VARCHAR(512);

	SELECT TOP 1

		@AddToGroupName			= [AG].[name]

	FROM

		sys.availability_groups AS [AG]

			INNER JOIN sys.availability_group_listeners AS [LS] ON [LS].[group_id] = [AG].[group_id]

				INNER JOIN sys.availability_group_listener_ip_addresses AS 172.16.23.120 ON 172.16.23.120.[listener_id] = [LS].[listener_id]

					INNER JOIN sys.dm_exec_connections AS [CN] ON [CN].[local_net_address] = 172.16.23.120.[ip_address] AND [CN].[local_tcp_port] = [LS].[port]

	WHERE

		[CN].[session_id] = @@SPID;

	SET @DatabaseName = (SELECT EVENTDATA().value('(/EVENT_INSTANCE/DatabaseName)[1]', 'nvarchar(128)'));

	-- We have to use a queue since initial backups cant happen during the CREATE DATABASE trigger firing.

	IF @AddToGroupName IS NOT NULL

		BEGIN

			PRINT 'Database is queueing for HADR replication';

			DELETE FROM hadr_pending_replicate WHERE database_name = @DatabaseName;

			INSERT INTO hadr_pending_replicate SELECT @DatabaseName, @AddToGroupName;

		END

END;

GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name='hadr_process_replicate')

	BEGIN

		DROP PROCEDURE hadr_process_replicate;

	END;

GO

/**

	* This script automatically performs a few tasks when a database is created via a connection

	* to a SQL Server availability group listener:

	*

	*     1) Switch the database from 'SIMPLE' to 'FULL' recovery.

	*	   2) Perform a full backup to the default backup path for the server.

	*     3) Connect to other servers in the availability group and stage a WITH NO RECOVERY restore.

	*	   4) Add the database to the availability group and initialize AlwaysOn.

	*

	* For this to work, you must have linked servers on all your nodes with the same name as the Windows

	* host name (i.e. SERVERNAME). If in doubt, look at replica_server_name from sys.availability_replicas.

	* Linked servers must have RPC and RPC OUT options set to true. Script assumes that the backup destination

	* is accessible to every other server too.

	**/

CREATE PROCEDURE hadr_process_replicate

	@AddToGroupName VARCHAR(2048),

	@DatabaseName VARCHAR(2048)

AS

BEGIN

	DECLARE @AvailabilityGroupID	UNIQUEIDENTIFIER;

	DECLARE @BackupDestination		VARCHAR(2048);

	DECLARE @BackupSuffix			VARCHAR(2048) = '_Initial.bak';

	SET @AvailabilityGroupID = (SELECT group_id FROM sys.availability_groups WHERE name=@AddToGroupName);

	-- Switch the database to FULL recovery if it was created without it.

	IF (SELECT recovery_model FROM sys.databases WHERE name=@DatabaseName) <> 1

		BEGIN

			PRINT 'Changing recovery model to FULL';

			DECLARE @ModeChange NVARCHAR(512) = 'ALTER DATABASE [' + @DatabaseName + ']  SET RECOVERY FULL WITH NO_WAIT';

			EXEC sp_executesql @ModeChange;

		END;

	ELSE

		BEGIN

			PRINT 'Database is already in FULL recovery mode.'

		END;

	-- Read the default backup path from the SQL Server configuration here. This path needs to be accessible to all the servers in

	-- the availability group.

	EXEC master.dbo.xp_instance_regread  N'HKEY_LOCAL_MACHINE',N'Software\Microsoft\MSSQLServer\MSSQLServer',N'BackupDirectory', @BackupDestination OUTPUT, 'no_output';

	DECLARE @TargetFile VARCHAR(2048) = @BackupDestination + '\' + @DatabaseName + @BackupSuffix;

	-- Perform initial backup of the database - Will overwrite any existing file.

	PRINT 'Backing up initial database to ' + @TargetFile;

	DECLARE @BackupCommand NVARCHAR(2048) = 'BACKUP DATABASE [' + @DatabaseName + '] TO  DISK = N''' + @TargetFile + ''' WITH INIT, NOFORMAT, NAME = N''Initial backup for HADR seeding'', SKIP, NOREWIND, NOUNLOAD,  STATS = 100';

	PRINT '    Command: ' + @BackupCommand

	EXEC (@BackupCommand)

	PRINT 'Joining database to availability group'

	DECLARE @JoinToAG NVARCHAR(2048) = 'ALTER AVAILABILITY GROUP [' + @AddToGroupName + '] ADD DATABASE [' + @DatabaseName + ']';

	EXEC sp_executesql @JoinToAG;

	-- Loop through all availability replicas

	DECLARE @Replicas TABLE (ReplicaName VARCHAR(512))

	INSERT INTO @Replicas	-- Have to use a table, since T-SQL wasnt giving me

							-- all replicas when I did this straight via the cursor.... (Bug?)

		SELECT replica_server_name

			FROM

				sys.availability_replicas

			WHERE group_id=CAST(@AvailabilityGroupID AS VARCHAR(512));

	DECLARE cur_Replicas CURSOR FOR SELECT ReplicaName FROM @Replicas INNER JOIN sys.servers [SV] ON [SV].[name] = [ReplicaName] AND [SV].[is_linked] = 1 ORDER BY [ReplicaName];

	OPEN cur_Replicas;

	DECLARE @CurrentReplica VARCHAR(255)

	FETCH NEXT FROM cur_Replicas INTO @CurrentReplica;

	WHILE @@FETCH_STATUS >= 0

		BEGIN

			PRINT 'Restoring initial backup to ' + @CurrentReplica;

			DECLARE @RestoreCommand VARCHAR(2048) = 'RESTORE DATABASE [' + @DatabaseName + '] FROM  DISK = N''' + @TargetFile + ''' WITH  NORECOVERY,  NOUNLOAD,  REPLACE,  STATS = 5;'

			-- The 'inception' moment.

			DECLARE @DoubleDynamicRestore NVARCHAR(2048) = 'EXEC (''' +  REPLACE(@RestoreCommand, '''', '''''') + ''') AT ' + @CurrentReplica;

			PRINT @DoubleDynamicRestore

			EXEC sp_executesql @DoubleDynamicRestore;

			PRINT 'Bringing replica online'

			DECLARE @DynamicAddHADR NVARCHAR(2048) = 'EXEC (''ALTER DATABASE [' + @DatabaseName + '] SET HADR AVAILABILITY GROUP = [' + @AddToGroupName +'];'') AT ' + @CurrentReplica;

			EXEC sp_executesql @DynamicAddHADR

			FETCH NEXT FROM cur_Replicas INTO @CurrentReplica;

		END;

	CLOSE cur_Replicas;

	DEALLOCATE cur_Replicas;

END;

GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name='hadr_replicate_queue')

	BEGIN

		DROP PROCEDURE hadr_replicate_queue;

	END;

GO

/**

	* Process all pending HADR replicates.

	**/

CREATE PROCEDURE hadr_replicate_queue

AS

BEGIN

	DECLARE cur_ReplicationTask CURSOR FOR

		SELECT database_name, availability_group_name FROM hadr_pending_replicate WITH(HOLDLOCK)

	OPEN cur_ReplicationTask;

	DECLARE @DB VARCHAR(512), @AG VARCHAR(2048)

	FETCH NEXT FROM cur_ReplicationTask INTO @DB, @AG

	WHILE @@FETCH_STATUS = 0

		BEGIN

			EXEC hadr_process_replicate @AddToGroupName = @AG, @DatabaseName = @DB

			FETCH NEXT FROM cur_ReplicationTask INTO @DB, @AG

		END;

	DELETE FROM hadr_pending_replicate;

	CLOSE cur_ReplicationTask;

	DEALLOCATE cur_ReplicationTask

END;

GO