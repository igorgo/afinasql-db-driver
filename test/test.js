/* eslint-disable no-console */
const Drv = require('../index'),
    fs = require('fs'),
    path = require('path'),
    assert = require('assert')

/**
 * @type {object}
 * @property afinaUser afinaUser
 * @property afinaWebPassword afinaWebPassword
 * @property afinaCompany afinaCompany
 * @property afinaApplication afinaApplication
 * @property afinaInterfaceLanguage afinaInterfaceLanguage
 * @property oldPackageSession oldPackageSession
 */
const config = JSON.parse(fs.readFileSync(path.join(__dirname, 'config.json')))

/**
 * Main test function
 * @returns {Promise.<void>} noting
 */
const tst = async () => {
    /**
     * @type AfinaSqlDbDriver
     */
    const db = new Drv.AfinaSqlDbDriver(config)
    assert.ok(!db.isOpened, 'Database is opened after ')
    let lRes
    await db.open()
    assert.ok(db.isOpened, 'Error open database')
    const sessionID = (await db.logon(config.afinaUser, config.afinaWebPassword, config.afinaCompany, config.afinaApplication, config.afinaInterfaceLanguage, config.oldPackageSession)).sessionID
    console.log('Logged in to Afina')
    assert.ok(sessionID, 'SessionID didn\'t recieve')
    console.log('Querying Companies...')
    lRes = await db.execute(sessionID, 'select * from v_companies')
    console.log(lRes.rows)
    console.log('Put the value 16 for the variable TEST_VAL to PKG_SESSION_VARS')
    let sql = `begin
                       PKG_SESSION_VARS.PUT(SNAME => :SNAME, NVALUE => :NVALUE);
                     end;`
    await db.execute(sessionID, sql, ['TEST_VAL', 16])
    console.log('Get a value of the the variable TEST_VAL from PKG_SESSION_VARS')
    sql = `begin
                       :RES := PKG_SESSION_VARS.GET_NUM(SNAME => :SNAME, NDEFVAL => :NDEFVAL);
                     end;`
    let params = new Drv.OraSqlParams()
    params.add('SNAME').dirIn().typeString().val('TEST_VAL')
    params.add('NDEFVAL').dirIn().typeNumber().val(0)
    params.add('RES').dirOut().typeNumber()
    lRes = (await db.execute(sessionID, sql, params)).outBinds['RES']
    assert.equal(lRes, 16, 'Put value not equal get value')
    console.log(lRes)
    await db.logoff(sessionID)
    console.log('Logged off from Afina')
    await db.close()
    assert.ok(!db.isOpened, 'Error close database')
}


tst()
    .then(() => process.exit(1))
    .catch(e => {
        console.log(e)
        process.exit(1)
    })