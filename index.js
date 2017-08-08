const _ = require('lodash'),
    /**
      * @type {oracledb}
      * @private
      */
    oci = require('oracle12db-win64')

/**
 * Oracle Bind Parameter
 */
class OraSqlParam {
    /**
     * Set the parameter's direction to IN
     * @returns {OraSqlParam} IN Param
     */
    dirIn() {
        this.dir = oci.BIND_IN
        return this
    }

    /**
     * Set the parameter's direction to OUT
     * @returns {OraSqlParam} OUT Param
     */
    dirOut() {
        this.dir = oci.BIND_OUT
        return this
    }

    /**
     * Set the parameter's direction to IN/OUT
     * @returns {OraSqlParam} IN/OUT Param
     */
    dirInOut() {
        this.dir = oci.BIND_INOUT
        return this
    }

    /**
     * Set the parameter's  datatype to NUMBER
     * @returns {OraSqlParam} number Param
     */
    typeNumber() {
        this.type = oci.NUMBER
        return this
    }

    /**
     * Set the parameter's  datatype to STRING
     * @param {number} [maxSize] max length of parameter. It's mandatory for OUT string params
     * @returns {OraSqlParam} varchar Param
     */
    typeString(maxSize) {
        this.type = oci.STRING
        if (maxSize) this.maxSize = maxSize
        return this
    }

    /**
     * Set the parameter's  datatype to DATE
     * @returns {OraSqlParam} date Param
     */
    typeDate() {
        this.type = oci.DATE
        return this
    }

    /**
     * Set the parameter's  datatype to CLOB
     * @returns {OraSqlParam} clob Param
     */
    typeClob() {
        this.type = oci.CLOB
        return this
    }

    /**
     * Set the parameter's  value
     * @param {*} value The Param's Value
     * @returns {OraSqlParam} Param with value
     */
    val(value) {
        this.val = value
        return this
    }
}

/**
 * Oracle Bind Parameters Collection
 */
class OraSqlParams {
    /**
     * Add parameter to collection
     * @param {string} name The Param's name
     * @returns {OraSqlParam} Added parameter
     */
    add(name) {
        let param = new OraSqlParam()
        _.set(this, name, param)
        return param
    }
}

/**
 * Oracle Database driver for Afina Sequel
 */
class AfinaSqlDbDriver {
    /**
     * Creates An insatance of the Oracle Database driver for Afina Sequel
     * @param {Object} config Database config
     * @param {string} config.username The login name of user
     * @param {string} config.password The login password of user
     * @param {string} config.connectString Connection string (tnsnames.ora entry)
     * @param {string} config.schema Session schema
     */
    constructor(config) {
        /**
         * @type {oracledbCLib.Oracledb}
         * @private
         */
        this._db = oci
        this._db.outFormat = this._db.OBJECT
        this._db.maxRows = 10000
        this._implementation = 'AdminOnline'
        this._db.fetchAsString = [this._db.CLOB]
        this._isOpened = false
        this._dbUser = config.username
        this._dbPassword = config.password
        this._dbConnectionString = config.connectString
        this._dbSchema = config.schema
        this._browser = 'Afina Oracle Driver'
    }

    /**
     * Check if the Oracle connections pool is open
     * @returns {boolean} True if the connections pool is open
     */
    get isOpened() {
        return this._isOpened
    }

    /**
     * Create Oracle connection pool
     * @returns {Promise.<AfinaSqlDbDriver>} An Instance of the driver with opened connections pool
     */
    async open() {
        if (!this._isOpened) {
            if (!this._dbUser) throw 'You must set user\'s name before open database'
            if (!this._dbConnectionString) throw 'You must set connect string before open database'
            this._pool = await this._db.createPool({
                user: this._dbUser,
                password: this._dbPassword,
                connectString: this._dbConnectionString
            })
            this._isOpened = true
        }
        return this
    }

    /**
     * Terminate Oracle connection pool
     * @returns {Promise.<AfinaSqlDbDriver>} An Instance of the driver with terminated connections pool
     */
    async close() {
        await this._pool.terminate()
        this._isOpened = false
        return this
    }

    /**
     * Logon to AfinaSql by utilizer
     * @param {string} aAfinaUser Afina's user name
     * @param {string} aAfinaWebPassword Afina's user web password
     * @param {string} [aAfinaCompany] Code of the session company
     * @param {string} [aAfinaApplication] Code of the Afina App, e.g. Admin, Balance …
     * @param {string} [aAfinaInterfaceLanguage] The session language (UKRAINIAN or RUSSIAN)
     * @param {boolean} [aOldPackageSession] use for "sail" connections
     * @returns {Promise.<logon>} New user session information
     * @property {number} NCOMPANY Session company RN
     * @property {string} SFULLUSERNAME Session user full name
     * @property {string} SAPPNAME Afina application name
     * @property {string} SCOMPANYFULLNAME Session company name
     * @property {string} sessionID  An Afina Sequel Session ID
     */
    async logon(aAfinaUser, aAfinaWebPassword, aAfinaCompany, aAfinaApplication, aAfinaInterfaceLanguage, aOldPackageSession = false) {
        const lSessionId = (await require('crypto').randomBytes(24)).toString('hex')
        const sqlLogon =
            `begin
               PKG_SESSION.LOGON_WEB(SCONNECT        => :SCONNECT,
                                     SUTILIZER       => :SUTILIZER,
                                     SPASSWORD       => :SPASSWORD,
                                     SIMPLEMENTATION => :SIMPLEMENTATION,
                                     SAPPLICATION    => :SAPPLICATION,
                                     SCOMPANY        => :SCOMPANY,
                                     ${!aOldPackageSession ? 'SBROWSER        => :SBROWSER,' : ''}
                                     SLANGUAGE       => :SLANGUAGE);
             end;`
        let paramsLogin = new OraSqlParams()
        paramsLogin.add('SCONNECT').val(lSessionId)
        paramsLogin.add('SUTILIZER').val(aAfinaUser)
        paramsLogin.add('SPASSWORD').val(aAfinaWebPassword)
        paramsLogin.add('SIMPLEMENTATION').val(this._implementation)
        paramsLogin.add('SAPPLICATION').val(aAfinaApplication)
        paramsLogin.add('SCOMPANY').val(aAfinaCompany)
        paramsLogin.add('SLANGUAGE').val(aAfinaInterfaceLanguage)
        !aOldPackageSession && paramsLogin.add('SBROWSER').val(this._browser)

        const sqlInfo =
            `select 
                PKG_SESSION.GET_COMPANY(0) as NCOMPANY, 
                PKG_SESSION.GET_UTILIZER_NAME() as SFULLUSERNAME, 
                PKG_SESSION.GET_APPLICATION_NAME(0) as SAPPNAME, 
                PKG_SESSION.GET_COMPANY_FULLNAME(0) as SCOMPANYFULLNAME 
            from dual`
        if (!this._isOpened) await this.open()
        let lConnection = await this._pool.getConnection()
        await lConnection.execute(`alter session set CURRENT_SCHEMA = ${this._dbSchema}`)
        try {
            await lConnection.execute(sqlLogon, paramsLogin, {})
            let resultInfo = (await lConnection.execute(sqlInfo, {}, {})).rows[0]
            _.set(resultInfo, 'sessionID', lSessionId)
            return resultInfo
        }
        finally {
            await lConnection.close()
        }
    }

    /**
     * Creates connection, sets the session schema, and changes session context to session utilizer
     * @param {string} aSessionId Afina Sequel Session ID
     * @returns {Promise.<oracledb.Connection>} Connection object is obtained by a Pool
     */
    async getConnection(aSessionId) {
        if (!this._isOpened) await this.open()
        let lConnection = await this._pool.getConnection()
        await lConnection.execute(`alter session set CURRENT_SCHEMA = ${this._dbSchema}`)
        await lConnection.execute('begin PKG_SESSION.VALIDATE_WEB(SCONNECT => :SCONNECT); end;', [aSessionId])
        return lConnection
    }

    /**
     * Executes a statement
     * @param {string} aSessionId An Afina Sequel Session ID
     * @param {string} aSql The SQL string that is executed. The SQL string may contain bind parameters.
     * @param {OraSqlParams|Array} [aBindParams] Definintion and values of the bind parameters. It's needed if there are bind parameters in the SQL statement
     * @param {{}|oracledb.IExecuteOptions} [aExecuteOptions] Execution options o control statement execution, such a fetchInfo, outFormat etc.
     * @param {oracledb.Connection} [aConnection] Existing connection. If it is set, then connection won't be closed, if not set the new connection will be open and will be closed after execution
     * @returns {Promise.<oracledb.IExecuteReturn>} The result Object. See https://github.com/oracle/node-oracledb/blob/master/doc/api.md#-result-object-properties
     */
    async execute(aSessionId, aSql, aBindParams = [], aExecuteOptions = {}, aConnection = null) {
        const lConnection = aConnection ? aConnection : (await this.getConnection(aSessionId))
        try {
            return await lConnection.execute(aSql, aBindParams, aExecuteOptions)
        }
        finally {
            aConnection || await lConnection.close()
        }
    }

    /**
     * Logs off from AfinaSql
     * @param {string} aSessionId An Afina Sequel Session ID
     * @returns {Promise.<number>} 0 if no errors, -1 if some error occurs
     */
    async logoff(aSessionId) {
        try {
            await this.execute(aSessionId, 'begin PKG_SESSION.LOGOFF_WEB(SCONNECT => :SCONNECT); end;', [aSessionId])
            return 0
        }
        catch (e) {
            return -1
        }
    }
}

module.exports = {AfinaSqlDbDriver, OraSqlParams}