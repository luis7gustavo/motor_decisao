"""Read-only structural acceptance for the explicit C1 migration, RT-014."""
import hashlib
import json

C1_TABLES = {
    'commercial.system_settings', 'control.ingestion_jobs', 'bronze.pncp_responses',
    'procurement.procurements', 'procurement.procurement_versions',
    'procurement.procurement_items', 'procurement.judgment_units',
    'procurement.judgment_unit_items', 'procurement.version_observations',
}


def verify_c1(conn, *, installed):
    names = conn.execute("""SELECT n.nspname||'.'||c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r'
        AND n.nspname NOT IN ('pg_catalog','information_schema')""").fetchall()
    present = C1_TABLES.intersection(row[0] for row in names)
    if present != (C1_TABLES if installed else set()):
        raise ValueError('C1 table inventory mismatch')
    functions = conn.execute("""SELECT p.proname,pg_get_functiondef(p.oid)
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='procurement' ORDER BY 1""").fetchall()
    if not installed:
        schemas = conn.execute("SELECT nspname FROM pg_namespace WHERE nspname IN ('procurement','commercial')").fetchall()
        if functions or schemas:
            raise ValueError('Downgrade left C1 schema/functions')
        return {'installed': False, 'new_objects_absent': True}
    if {row[0] for row in functions} != {'reject_mutation','guard_version','guard_child','guard_current'}:
        raise ValueError('C1 functions missing')
    triggers = conn.execute("""SELECT n.nspname,c.relname,t.tgname,pg_get_triggerdef(t.oid)
        FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE NOT t.tgisinternal AND n.nspname||'.'||c.relname=ANY(%s) ORDER BY 1,2,3""", (sorted(C1_TABLES),)).fetchall()
    if len(triggers) != 8:
        raise ValueError('C1 integrity triggers missing')
    foreign_keys = conn.execute("""SELECT c.conname,c.confdeltype FROM pg_constraint c
        WHERE c.contype='f' AND c.conrelid IN
        (SELECT cl.oid FROM pg_class cl JOIN pg_namespace n ON n.oid=cl.relnamespace
         WHERE n.nspname||'.'||cl.relname=ANY(%s)) ORDER BY 1""", (sorted(C1_TABLES),)).fetchall()
    if not foreign_keys or any(row[1] != 'r' for row in foreign_keys):
        raise ValueError('All C1 foreign keys must be RESTRICT')
    return {'installed': True, 'table_count': len(present), 'trigger_count': len(triggers),
            'restrict_fk_count': len(foreign_keys),
            'function_trigger_hash': hashlib.sha256(json.dumps([functions,triggers]).encode()).hexdigest()}
