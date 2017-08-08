const _ = require('lodash'),
    /**
     * @type {oracledb}
     */
    oci = require('oracle12db-win64')


class OraSqlParam {
    dirIn() {
        this.dir = oci.BIND_IN
        return this
    }

    dirOut() {
        this.dir = oci.BIND_OUT
        return this
    }

    dirInOut() {
        this.dir = oci.BIND_INOUT
        return this
    }

    typeNumber() {
        this.type = oci.NUMBER
        return this
    }

    typeString(maxSize) {
        this.type = oci.STRING
        if (maxSize) this.maxSize = maxSize
        return this
    }

    typeDate() {
        this.type = oci.DATE
        return this
    }

    typeClob() {
        this.type = oci.CLOB
        return this
    }

    val(value) {
        this.val = value
        return this
    }
}

class OraSqlParams {
    add(name) {
        let param = new OraSqlParam()
        _.set(this, name, param)
        return param
    }
}

class AfinaSqlDbDriver {
    /**
     * Creates An Oracle Database Driver for Afina Sequel
     * @param {Object} config
     * @param {string} config.username
     * @param {string} config.password
     * @param {string} config.connectString
     * @param {string} config.schema
     */
    constructor(config) {
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
     * @returns {boolean}
     */
    get isOpened() {
        return this._isOpened
    }

    /**
     * Opens the database
     * @returns {Promise.<AfinaSqlDbDriver>}
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
     * Closes the database
     * @returns {Promise.<AfinaSqlDbDriver>}
     */
    async close() {
        await this._pool.terminate()
        this._isOpened = false
        return this
    }

    /**
     * Returns a Connection object is obtained by a Pool
     * @param {string} aSessionId Afina Sequel Session ID
     * @returns {Promise.<oracledb.Connection>}
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
     * @param {string} aSql A statement
     * @param {OraSqlParams|Array} [aBindParams]
     * @param {{}|oracledb.IExecuteOptions} [aExecuteOptions]
     * @param {oracledb.Connection} [aConnection]
     * @returns {Promise.<oracledb.IExecuteReturn>}
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
     * @typedef {Object} SessionInfo
     * @property {number} NCOMPANY
     * @property {string} SFULLUSERNAME
     * @property {string} SAPPNAME
     * @property {string} SCOMPANYFULLNAME
     * @property {string} sessionID
     */

    /**
     * Logon to AfinaSql by utilizer
     * @param {string} aAfinaUser
     * @param {string} aAfinaWebPassword
     * @param {string} [aAfinaCompany]
     * @param {string} [aAfinaApplication]
     * @param {string} [aAfinaInterfaceLanguage]
     * @param {boolean} [aOldPackageSession] use for "sail" connections
     * @returns {Promise.<SessionInfo>}
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
     * Logs off from AfinaSql
     * @param {string} aSessionId
     * @returns {Promise.<number>}
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