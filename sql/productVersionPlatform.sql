SELECT  pt_TEXT_VALUE.ID_OBJ
       ,[Key]
       ,[Name]
       ,[Platform]
       ,[Product Version]
       ,[Status]
       ,[Created]
       ,[Updated]
FROM
(
	SELECT  a.ID_OBJ
	       ,v.[VALUE]
	       ,a.NAME attrName
	FROM jiraschema.AO_8542F1_IFJ_OBJ_ATTR_VAL v
	JOIN
	(
		SELECT  a.[OBJECT_ID] ID_OBJ
		       ,a.ID ID_OBJ_ATTR
		       ,ota.id ID_OBJ_TYPE_ATTR
		       ,ota.[NAME]
		FROM jiraschema.AO_8542F1_IFJ_OBJ_TYPE_ATTR ota
		JOIN jiraschema.AO_8542F1_IFJ_OBJ_ATTR a
		ON a.OBJECT_TYPE_ATTRIBUTE_ID = ota.ID AND a.UPDATED > @lastUpdate
		JOIN jiraschema.AO_8542F1_IFJ_OBJ obj
		ON obj.ID = a.[OBJECT_ID]
		JOIN jiraschema.AO_8542F1_IFJ_OBJ_TYPE t
		ON ( t.ID = obj.OBJECT_TYPE_ID AND t.NAME = 'Product Version Platform')
	) a
	ON v.OBJECT_ATTRIBUTE_ID = a.ID_OBJ_ATTR
	WHERE v.[VALUE] is not NULL 
) attr PIVOT(MAX(attr.[VALUE]) FOR attr.attrName IN ( [Key] , [Name] , [Platform] , [Product Version] , [Status] , [Created] , [Updated])) AS pt_TEXT_VALUE