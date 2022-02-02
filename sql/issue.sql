SELECT
    ji.IssueID
    ,ji.IssueKey
	,ji.Summary
	,(SELECT cf.STRINGVALUE FROM jiraschema.customfield cf
		WHERE   cf.issue = ji.IssueID AND  cf.cfname = 'Company') Company
	,(SELECT cf.STRINGVALUE FROM jiraschema.customfield cf
        WHERE   cf.issue = ji.IssueID AND  cf.cfname = 'Operating System') ProductVersionPlatform
	,(SELECT cf.STRINGVALUE FROM jiraschema.customfield cf
        WHERE   cf.issue = ji.IssueID AND  cf.cfname = 'Contact') Contact
	, ji.ResolutionDate
	, ji.Created
	, ji.Updated
 FROM jiraschema.jiraissue ji