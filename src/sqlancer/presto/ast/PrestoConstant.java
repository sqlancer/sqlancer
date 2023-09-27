package sqlancer.presto.ast;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoConstantUtils;
import sqlancer.presto.PrestoSchema;

public abstract class PrestoConstant implements Node<PrestoExpression>, PrestoExpression {

    private static final String[] TIME_ZONES = { "Africa/Abidjan", "Africa/Accra", "Africa/Addis_Ababa",
            "Africa/Algiers", "Africa/Asmara", "Africa/Asmera", "Africa/Bamako", "Africa/Bangui", "Africa/Banjul",
            "Africa/Bissau", "Africa/Blantyre", "Africa/Brazzaville", "Africa/Bujumbura", "Africa/Cairo",
            "Africa/Casablanca", "Africa/Ceuta", "Africa/Conakry", "Africa/Dakar", "Africa/Dar_es_Salaam",
            "Africa/Djibouti", "Africa/Douala", "Africa/El_Aaiun", "Africa/Freetown", "Africa/Gaborone",
            "Africa/Harare", "Africa/Johannesburg", "Africa/Juba", "Africa/Kampala", "Africa/Khartoum", "Africa/Kigali",
            "Africa/Kinshasa", "Africa/Lagos", "Africa/Libreville", "Africa/Lome", "Africa/Luanda", "Africa/Lubumbashi",
            "Africa/Lusaka", "Africa/Malabo", "Africa/Maputo", "Africa/Maseru", "Africa/Mbabane", "Africa/Mogadishu",
            "Africa/Monrovia", "Africa/Nairobi", "Africa/Ndjamena", "Africa/Niamey", "Africa/Nouakchott",
            "Africa/Ouagadougou", "Africa/Porto-Novo", "Africa/Sao_Tome", "Africa/Timbuktu", "Africa/Tripoli",
            "Africa/Tunis", "Africa/Windhoek", "America/Adak", "America/Anchorage", "America/Anguilla",
            "America/Antigua", "America/Araguaina", "America/Argentina/Buenos_Aires", "America/Argentina/Catamarca",
            "America/Argentina/ComodRivadavia", "America/Argentina/Cordoba", "America/Argentina/Jujuy",
            "America/Argentina/La_Rioja", "America/Argentina/Mendoza", "America/Argentina/Rio_Gallegos",
            "America/Argentina/Salta", "America/Argentina/San_Juan", "America/Argentina/San_Luis",
            "America/Argentina/Tucuman", "America/Argentina/Ushuaia", "America/Aruba", "America/Asuncion",
            "America/Atikokan", "America/Atka", "America/Bahia", "America/Barbados", "America/Belem", "America/Belize",
            "America/Blanc-Sablon", "America/Boa_Vista", "America/Bogota", "America/Boise", "America/Buenos_Aires",
            "America/Cambridge_Bay", "America/Campo_Grande", "America/Cancun", "America/Caracas", "America/Catamarca",
            "America/Cayenne", "America/Cayman", "America/Chicago", "America/Chihuahua", "America/Coral_Harbour",
            "America/Cordoba", "America/Costa_Rica", "America/Creston", "America/Cuiaba", "America/Curacao",
            "America/Danmarkshavn", "America/Dawson", "America/Dawson_Creek", "America/Denver", "America/Detroit",
            "America/Dominica", "America/Edmonton", "America/Eirunepe", "America/El_Salvador", "America/Ensenada",
            "America/Fort_Nelson", "America/Fort_Wayne", "America/Fortaleza", "America/Glace_Bay", "America/Godthab",
            "America/Goose_Bay", "America/Grand_Turk", "America/Grenada", "America/Guadeloupe", "America/Guatemala",
            "America/Guayaquil", "America/Guyana", "America/Halifax", "America/Havana", "America/Hermosillo",
            "America/Indiana/Indianapolis", "America/Indiana/Knox", "America/Indiana/Marengo",
            "America/Indiana/Petersburg", "America/Indiana/Tell_City", "America/Indiana/Vevay",
            "America/Indiana/Vincennes", "America/Indiana/Winamac", "America/Indianapolis", "America/Inuvik",
            "America/Iqaluit", "America/Jamaica", "America/Jujuy", "America/Juneau", "America/Kentucky/Louisville",
            "America/Kentucky/Monticello", "America/Knox_IN", "America/Kralendijk", "America/La_Paz", "America/Lima",
            "America/Los_Angeles", "America/Louisville", "America/Lower_Princes", "America/Maceio", "America/Managua",
            "America/Manaus", "America/Marigot", "America/Martinique", "America/Matamoros", "America/Mendoza",
            "America/Menominee", "America/Merida", "America/Metlakatla", "America/Mexico_City", "America/Miquelon",
            "America/Moncton", "America/Monterrey", "America/Montevideo", "America/Montreal", "America/Montserrat",
            "America/Nassau", "America/New_York", "America/Nipigon", "America/Nome", "America/Noronha",
            "America/North_Dakota/Beulah", "America/North_Dakota/Center", "America/North_Dakota/New_Salem",
            "America/Nuuk", "America/Ojinaga", "America/Panama", "America/Pangnirtung", "America/Paramaribo",
            "America/Phoenix", "America/Port-au-Prince", "America/Port_of_Spain", "America/Porto_Acre",
            "America/Porto_Velho", "America/Puerto_Rico", "America/Punta_Arenas", "America/Rainy_River",
            "America/Rankin_Inlet", "America/Recife", "America/Regina", "America/Resolute", "America/Rio_Branco",
            "America/Rosario", "America/Santa_Isabel", "America/Santarem", "America/Santiago", "America/Santo_Domingo",
            "America/Sao_Paulo", "America/Scoresbysund", "America/Shiprock", "America/Sitka", "America/St_Barthelemy",
            "America/St_Johns", "America/St_Kitts", "America/St_Lucia", "America/St_Thomas", "America/St_Vincent",
            "America/Swift_Current", "America/Tegucigalpa", "America/Thule", "America/Thunder_Bay", "America/Tijuana",
            "America/Toronto", "America/Tortola", "America/Vancouver", "America/Virgin", "America/Whitehorse",
            "America/Winnipeg", "America/Yakutat", "America/Yellowknife", "Antarctica/Casey", "Antarctica/Davis",
            "Antarctica/DumontDUrville", "Antarctica/Macquarie", "Antarctica/Mawson", "Antarctica/McMurdo",
            "Antarctica/Palmer", "Antarctica/Rothera", "Antarctica/South_Pole", "Antarctica/Syowa", "Antarctica/Troll",
            "Antarctica/Vostok", "Arctic/Longyearbyen", "Asia/Aden", "Asia/Almaty", "Asia/Amman", "Asia/Anadyr",
            "Asia/Aqtau", "Asia/Aqtobe", "Asia/Ashgabat", "Asia/Ashkhabad", "Asia/Atyrau", "Asia/Baghdad",
            "Asia/Bahrain", "Asia/Baku", "Asia/Bangkok", "Asia/Barnaul", "Asia/Beirut", "Asia/Bishkek", "Asia/Brunei",
            "Asia/Calcutta", "Asia/Chita", "Asia/Choibalsan", "Asia/Chongqing", "Asia/Chungking", "Asia/Colombo",
            "Asia/Dacca", "Asia/Dhaka", "Asia/Dili", "Asia/Dubai", "Asia/Dushanbe", "Asia/Famagusta", "Asia/Gaza",
            "Asia/Harbin", "Asia/Hebron", "Asia/Ho_Chi_Minh", "Asia/Hong_Kong", "Asia/Hovd", "Asia/Irkutsk",
            "Asia/Istanbul", "Asia/Jakarta", "Asia/Jayapura", "Asia/Jerusalem", "Asia/Kabul", "Asia/Kamchatka",
            "Asia/Karachi", "Asia/Kashgar", "Asia/Kathmandu", "Asia/Katmandu", "Asia/Khandyga", "Asia/Kolkata",
            "Asia/Krasnoyarsk", "Asia/Kuala_Lumpur", "Asia/Kuching", "Asia/Kuwait", "Asia/Macao", "Asia/Macau",
            "Asia/Magadan", "Asia/Makassar", "Asia/Manila", "Asia/Muscat", "Asia/Nicosia", "Asia/Novokuznetsk",
            "Asia/Novosibirsk", "Asia/Omsk", "Asia/Oral", "Asia/Phnom_Penh", "Asia/Pontianak", "Asia/Pyongyang",
            "Asia/Qatar", "Asia/Qostanay", "Asia/Qyzylorda", "Asia/Rangoon", "Asia/Riyadh", "Asia/Saigon",
            "Asia/Sakhalin", "Asia/Samarkand", "Asia/Seoul", "Asia/Shanghai", "Asia/Singapore", "Asia/Srednekolymsk",
            "Asia/Taipei", "Asia/Tashkent", "Asia/Tbilisi", "Asia/Tehran", "Asia/Tel_Aviv", "Asia/Thimbu",
            "Asia/Thimphu", "Asia/Tokyo", "Asia/Tomsk", "Asia/Ujung_Pandang", "Asia/Ulaanbaatar", "Asia/Ulan_Bator",
            "Asia/Urumqi", "Asia/Ust-Nera", "Asia/Vientiane", "Asia/Vladivostok", "Asia/Yakutsk", "Asia/Yangon",
            "Asia/Yekaterinburg", "Asia/Yerevan", "Atlantic/Azores", "Atlantic/Bermuda", "Atlantic/Canary",
            "Atlantic/Cape_Verde", "Atlantic/Faeroe", "Atlantic/Faroe", "Atlantic/Jan_Mayen", "Atlantic/Madeira",
            "Atlantic/Reykjavik", "Atlantic/South_Georgia", "Atlantic/St_Helena", "Atlantic/Stanley", "Australia/ACT",
            "Australia/Adelaide", "Australia/Brisbane", "Australia/Broken_Hill", "Australia/Canberra",
            "Australia/Currie", "Australia/Darwin", "Australia/Eucla", "Australia/Hobart", "Australia/LHI",
            "Australia/Lindeman", "Australia/Lord_Howe", "Australia/Melbourne", "Australia/North", "Australia/Perth",
            "Australia/Queensland", "Australia/South", "Australia/Sydney", "Australia/Tasmania", "Australia/Victoria",
            "Australia/West", "Australia/Yancowinna", "Brazil/Acre", "Brazil/DeNoronha", "Brazil/East", "Brazil/West",
            "CET", "CST6CDT", "Canada/Atlantic", "Canada/Central", "Canada/Eastern", "Canada/Mountain",
            "Canada/Newfoundland", "Canada/Pacific", "Canada/Saskatchewan", "Canada/Yukon", "Chile/Continental",
            "Chile/EasterIsland", "Cuba", "EET", "EST5EDT", "Egypt", "Eire", "Etc/GMT", "Etc/GMT+0", "Etc/GMT+1",
            "Etc/GMT+10", "Etc/GMT+11", "Etc/GMT+12", "Etc/GMT+2", "Etc/GMT+3", "Etc/GMT+4", "Etc/GMT+5", "Etc/GMT+6",
            "Etc/GMT+7", "Etc/GMT+8", "Etc/GMT+9", "Etc/GMT-0", "Etc/GMT-1", "Etc/GMT-10", "Etc/GMT-11", "Etc/GMT-12",
            "Etc/GMT-13", "Etc/GMT-14", "Etc/GMT-2", "Etc/GMT-3", "Etc/GMT-4", "Etc/GMT-5", "Etc/GMT-6", "Etc/GMT-7",
            "Etc/GMT-8", "Etc/GMT-9", "Etc/GMT0", "Etc/Greenwich", "Etc/UCT", "Etc/UTC", "Etc/Universal", "Etc/Zulu",
            "Europe/Amsterdam", "Europe/Andorra", "Europe/Astrakhan", "Europe/Athens", "Europe/Belfast",
            "Europe/Belgrade", "Europe/Berlin", "Europe/Bratislava", "Europe/Brussels", "Europe/Bucharest",
            "Europe/Budapest", "Europe/Busingen", "Europe/Chisinau", "Europe/Copenhagen", "Europe/Dublin",
            "Europe/Gibraltar", "Europe/Guernsey", "Europe/Helsinki", "Europe/Isle_of_Man", "Europe/Istanbul",
            "Europe/Jersey", "Europe/Kaliningrad", "Europe/Kiev", "Europe/Kirov", "Europe/Lisbon", "Europe/Ljubljana",
            "Europe/London", "Europe/Luxembourg", "Europe/Madrid", "Europe/Malta", "Europe/Mariehamn", "Europe/Minsk",
            "Europe/Monaco", "Europe/Moscow", "Europe/Nicosia", "Europe/Oslo", "Europe/Paris", "Europe/Podgorica",
            "Europe/Prague", "Europe/Riga", "Europe/Rome", "Europe/Samara", "Europe/San_Marino", "Europe/Sarajevo",
            "Europe/Saratov", "Europe/Simferopol", "Europe/Skopje", "Europe/Sofia", "Europe/Stockholm", "Europe/Tirane",
            "Europe/Tiraspol", "Europe/Ulyanovsk", "Europe/Uzhgorod", "Europe/Vaduz", "Europe/Vatican", "Europe/Vienna",
            "Europe/Vilnius", "Europe/Volgograd", "Europe/Warsaw", "Europe/Zagreb", "Europe/Zaporozhye",
            "Europe/Zurich", "GB", "GB-Eire", "GMT", "GMT0", "Greenwich", "Hongkong", "Iceland", "Indian/Antananarivo",
            "Indian/Chagos", "Indian/Christmas", "Indian/Cocos", "Indian/Comoro", "Indian/Kerguelen", "Indian/Mahe",
            "Indian/Maldives", "Indian/Mauritius", "Indian/Mayotte", "Indian/Reunion", "Iran", "Israel", "Jamaica",
            "Japan", "Kwajalein", "Libya", "MET", "MST7MDT", "Mexico/General", "NZ", "NZ-CHAT", "Navajo", "PRC",
            "PST8PDT", "Pacific/Apia", "Pacific/Auckland", "Pacific/Bougainville", "Pacific/Chatham", "Pacific/Chuuk",
            "Pacific/Easter", "Pacific/Efate", "Pacific/Enderbury", "Pacific/Fakaofo", "Pacific/Fiji",
            "Pacific/Funafuti", "Pacific/Galapagos", "Pacific/Gambier", "Pacific/Guadalcanal", "Pacific/Guam",
            "Pacific/Honolulu", "Pacific/Johnston", "Pacific/Kiritimati", "Pacific/Kosrae", "Pacific/Kwajalein",
            "Pacific/Majuro", "Pacific/Marquesas", "Pacific/Midway", "Pacific/Nauru", "Pacific/Niue", "Pacific/Norfolk",
            "Pacific/Noumea", "Pacific/Pago_Pago", "Pacific/Palau", "Pacific/Pitcairn", "Pacific/Pohnpei",
            "Pacific/Ponape", "Pacific/Port_Moresby", "Pacific/Rarotonga", "Pacific/Saipan", "Pacific/Samoa",
            "Pacific/Tahiti", "Pacific/Tarawa", "Pacific/Tongatapu", "Pacific/Truk", "Pacific/Wake", "Pacific/Wallis",
            "Pacific/Yap", "Poland", "Portugal", "ROK", "Singapore", "Turkey", "UCT", "US/Alaska", "US/Aleutian",
            "US/Arizona", "US/Central", "US/East-Indiana", "US/Eastern", "US/Hawaii", "US/Indiana-Starke",
            "US/Michigan", "US/Mountain", "US/Pacific", "US/Samoa", "UTC", "Universal", "W-SU", "WET", "Zulu" };
    private static final String FALSE = "false";
    private static final String TRUE = "true";

    private PrestoConstant() {
    }

    public static Node<PrestoExpression> createStringConstant(String text) {
        return new PrestoTextConstant(text);
    }

    public static Node<PrestoExpression> createStringConstant(String text, int size) {
        return new PrestoTextConstant(text, size);
    }

    public static Node<PrestoExpression> createJsonConstant() {
        return new PrestoJsonConstant();
    }

    public static Node<PrestoExpression> createFloatConstant(PrestoSchema.PrestoCompositeDataType type, double val) {
        assert type.getSize() == 4;
        float floatValue = (float) val;
        return new PrestoFloatConstant(floatValue);
    }

    public static Node<PrestoExpression> createFloatConstant(double val) {
        return new PrestoFloatConstant(val);
    }

    public static Node<PrestoExpression> createDecimalConstant(double val) {
        return new PrestoDecimalConstant(val);
    }

    public static Node<PrestoExpression> createDecimalConstant(PrestoSchema.PrestoCompositeDataType type, double val) {
        int scale = type.getScale();
        int precision = type.getSize();
        BigDecimal finalBD = PrestoConstantUtils.getDecimal(val, scale, precision);
        return new PrestoDecimalConstant(finalBD.doubleValue());
    }

    public static Node<PrestoExpression> createIntConstant(long val) {
        return new PrestoIntConstant(val);
    }

    public static Node<PrestoExpression> createIntConstant(PrestoSchema.PrestoCompositeDataType type, long val,
            boolean castInteger) {
        PrestoIntConstant intConstant;
        assert List.of(1, 2, 4, 8).contains(type.getSize());
        switch (type.getSize()) {
        case 1:
            intConstant = new PrestoIntConstant((byte) val);
            break;
        case 2:
            intConstant = new PrestoIntConstant((short) val);
            break;
        case 4:
            intConstant = new PrestoIntConstant((int) val);
            break;
        default:
            intConstant = new PrestoIntConstant(val);
        }
        if (castInteger) {
            return new PrestoCastFunction(intConstant, type);
        } else {
            return intConstant;
        }
    }

    public static Node<PrestoExpression> createNullConstant() {
        return new PrestoNullConstant();
    }

    public static Node<PrestoExpression> createBooleanConstant(boolean val) {
        return new PrestoBooleanConstant(val);
    }

    public static Node<PrestoExpression> createDateConstant(long integer) {
        return new PrestoDateConstant(integer);
    }

    public static Node<PrestoExpression> createTimeConstant(long integer) {
        return new PrestoTimeConstant(integer);
    }

    public static Node<PrestoExpression> createTimeWithTimeZoneConstant(long integer) {
        return new PrestoTimeWithTimeZoneConstant(integer);
    }

    public static Node<PrestoExpression> createTimestampWithTimeZoneConstant(long integer) {
        return new PrestoTimestampWithTimezoneConstant(integer);
    }

    public static Node<PrestoExpression> createIntervalDayToSecond(long integer) {
        return new PrestoIntervalDayToSecondConstant();
    }

    public static Node<PrestoExpression> createIntervalYearToMonth(long integer) {
        return new PrestoIntervalYearToMonthConstant();
    }

    public static Node<PrestoExpression> createTimestampConstant(long integer) {
        return new PrestoTimestampConstant(integer);
    }

    public static Node<PrestoExpression> createVarbinaryConstant(String string) {
        return new PrestoVarbinaryConstant(string);
    }

    public static Node<PrestoExpression> createTimezoneConstant() {
        String string = Randomly.fromOptions(TIME_ZONES);
        return new PrestoTextConstant(string);
    }

    public static Node<PrestoExpression> createArrayConstant(PrestoSchema.PrestoCompositeDataType type) {
        PrestoSchema.PrestoCompositeDataType elementType = type.getElementType();
        long size = Randomly.getNotCachedInteger(0, 10);

        List<Node<PrestoExpression>> elements = new ArrayList<>();
        for (int i = 0; i <= size; i++) {
            if (elementType.getPrimitiveDataType() == PrestoSchema.PrestoDataType.ARRAY) {
                elements.add(createArrayConstant(elementType));
            } else {
                elements.add(generateConstant(elementType, false));
            }
        }
        return new PrestoArrayConstant(elements);
    }

    public static Node<PrestoExpression> createMapConstant(PrestoSchema.PrestoCompositeDataType type) {
        PrestoSchema.PrestoCompositeDataType elementType = type.getElementType();
        long size = Randomly.getNotCachedInteger(0, 10);

        List<Node<PrestoExpression>> elements = new ArrayList<>();
        for (int i = 0; i <= size; i++) {
            if (elementType.getPrimitiveDataType() == PrestoSchema.PrestoDataType.ARRAY) {
                elements.add(createArrayConstant(elementType));
            } else {
                elements.add(generateConstant(elementType, false));
            }
        }
        return new PrestoArrayConstant(elements);
    }

    public static Node<PrestoExpression> generateConstant(PrestoSchema.PrestoCompositeDataType type,
            boolean castInteger) {
        Randomly randomly = new Randomly();
        switch (type.getPrimitiveDataType()) {
        case ARRAY:
            return PrestoConstant.createArrayConstant(type);
        case NULL:
            return PrestoConstant.createNullConstant();
        case CHAR:
            return PrestoConstant.PrestoTextConstant.createStringConstant(randomly.getAlphabeticChar(), type.getSize());
        case VARCHAR:
            return PrestoConstant.PrestoTextConstant.createStringConstant(randomly.getString(), type.getSize());
        case VARBINARY:
            return PrestoConstant.createVarbinaryConstant(randomly.getString());
        case JSON:
            return PrestoConstant.PrestoJsonConstant.createJsonConstant();
        case TIME:
            return PrestoConstant.createTimeConstant(randomly.getLong(0, System.currentTimeMillis()));
        case TIME_WITH_TIME_ZONE:
            return PrestoConstant.createTimeWithTimeZoneConstant(randomly.getLong(0, System.currentTimeMillis()));
        case TIMESTAMP:
            return PrestoConstant.createTimestampConstant(randomly.getLong(0, System.currentTimeMillis()));
        case TIMESTAMP_WITH_TIME_ZONE:
            return PrestoConstant.createTimestampWithTimeZoneConstant(randomly.getLong(0, System.currentTimeMillis()));
        case INTERVAL_YEAR_TO_MONTH:
            return PrestoConstant.createIntervalYearToMonth(randomly.getLong(0, System.currentTimeMillis()));
        case INTERVAL_DAY_TO_SECOND:
            return PrestoConstant.createIntervalDayToSecond(randomly.getLong(0, System.currentTimeMillis()));
        case INT:
            return PrestoConstant.PrestoIntConstant.createIntConstant(type, Randomly.getNonCachedInteger(),
                    castInteger);
        case FLOAT:
            return PrestoConstant.PrestoFloatConstant.createFloatConstant(randomly.getDouble());
        case BOOLEAN:
            return PrestoConstant.PrestoBooleanConstant.createBooleanConstant(Randomly.getBoolean());
        case DATE:
            return PrestoConstant.createDateConstant(randomly.getLong(0, System.currentTimeMillis()));
        case DECIMAL:
            return PrestoConstant.createDecimalConstant(type, randomly.getLong(0, System.currentTimeMillis()));
        default:
            throw new AssertionError("Unknown type: " + type);
        }
    }

    public boolean isNull() {
        return false;
    }

    public boolean isInt() {
        return false;
    }

    public boolean isBoolean() {
        return false;
    }

    public boolean isArray() {
        return false;
    }

    public boolean isString() {
        return false;
    }

    public boolean isFloat() {
        return false;
    }

    public boolean asBoolean() {
        throw new UnsupportedOperationException(this.toString());
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.toString());
    }

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public double asFloat() {
        throw new UnsupportedOperationException(this.toString());
    }

    public static class PrestoNullConstant extends PrestoConstant {

        @Override
        public String toString() {
            return "NULL";
        }

        @Override
        public boolean isNull() {
            return true;
        }

    }

    public static class PrestoIntConstant extends PrestoConstant {

        private final long value;

        public PrestoIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

        @Override
        public boolean isInt() {
            return true;
        }

    }

    public static class PrestoFloatConstant extends PrestoConstant {

        private final double value;

        public PrestoFloatConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "infinity()";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "-infinity()";
            }
            return String.valueOf(value);
        }

        @Override
        public boolean isFloat() {
            return true;
        }

        @Override
        public double asFloat() {
            return value;
        }

    }

    public static class PrestoDecimalConstant extends PrestoConstant {

        private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###0.0000");

        private final double value;

        public PrestoDecimalConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return DECIMAL_FORMAT.format(value);
        }

        @Override
        public double asFloat() {
            return value;
        }

    }

    public static class PrestoTextConstant extends PrestoConstant {

        private final String value;

        public PrestoTextConstant(String value) {
            this.value = value;
        }

        public PrestoTextConstant(String value, int size) {
            this.value = value.substring(0, Math.min(value.length(), size));
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("'", "''") + "'";
        }

    }

    public static class PrestoVarbinaryConstant extends PrestoConstant {

        private final String value;

        public PrestoVarbinaryConstant(String value) {
            this.value = value.replace("'", "");
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.format("CAST ('%s' AS VARBINARY)", value);
        }

    }

    public static class PrestoJsonConstant extends PrestoConstant {

        private final String value;

        public PrestoJsonConstant() {
            Randomly rand = new Randomly();
            JsonValueType jvt = Randomly.fromOptions(JsonValueType.values());
            String val;
            switch (jvt) {
            case NULL:
                val = "null";
                value = "{\"val\":" + val + "}";
                break;
            case FALSE:
                val = FALSE;
                value = "{\"val\":" + val + "}";
                break;
            case TRUE:
                val = TRUE;
                value = "{\"val\":" + val + "}";
                break;
            case STRING:
                String randString = rand.getString();
                String string = randString.substring(0, Math.min(randString.length(), 250));
                string = string.replace("'", "");
                // https://www.rfc-editor.org/rfc/rfc8259#page-8
                string = PrestoConstantUtils.removeAllControlChars(string);
                string = string.replace("\\", "\\\\");

                value = "{\"val\": \"" + string + "\"}";
                break;
            case NUMBER:
                if (Randomly.getBoolean()) {
                    int no = (int) rand.getInteger();
                    val = String.valueOf(no);
                } else {
                    double no = rand.getDouble();
                    val = String.valueOf(no);
                }
                value = "{\"val\": " + val + "}";
                break;
            case ARRAY:
                value = "{\"employees\":[\"John\", \"Anna\", \"Peter\"]}";
                break;
            case OBJECT:
                value = "{\"employee\":{\"name\":\"John\", \"age\":30, \"city\":\"New York\"}}";
                break;
            default:
                value = "{}";
            }
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "JSON '" + value + "'";
        }

        private enum JsonValueType {
            OBJECT, ARRAY, NUMBER, STRING, TRUE, FALSE, NULL
        }

    }

    public static class PrestoDateConstant extends PrestoConstant {

        private final String textRepresentation;

        public PrestoDateConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            textRepresentation = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepresentation;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepresentation);
        }

    }

    public static class PrestoTimeConstant extends PrestoConstant {

        public final String textRepresentation;

        public PrestoTimeConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
            textRepresentation = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepresentation;
        }

        @Override
        public String toString() {
            return String.format("TIME '%s'", textRepresentation);
        }

    }

    public static class PrestoTimeWithTimeZoneConstant extends PrestoConstant {

        private final String textRepresentation;
        private final String timeZone;

        public PrestoTimeWithTimeZoneConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
            textRepresentation = dateFormat.format(timestamp);
            this.timeZone = Randomly.fromOptions(TIME_ZONES);
        }

        public String getValue() {
            return textRepresentation;
        }

        @Override
        public String toString() {
            return String.format("TIME '%s %s'", textRepresentation, timeZone);
        }

    }

    public static class PrestoTimestampConstant extends PrestoConstant {

        private final String textRepresentation;

        public PrestoTimestampConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.textRepresentation = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepresentation;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepresentation);
        }

    }

    public static class PrestoTimestampWithTimezoneConstant extends PrestoConstant {

        private final String textRepresentation;
        private final String timeZone;

        public PrestoTimestampWithTimezoneConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.textRepresentation = dateFormat.format(timestamp);
            this.timeZone = Randomly.fromOptions(TIME_ZONES);
        }

        public String getValue() {
            return textRepresentation;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s %s'", textRepresentation, timeZone);
        }

    }

    public static class PrestoIntervalDayToSecondConstant extends PrestoConstant {

        private final String textRepresentation;
        private final Interval fromInterval;

        public PrestoIntervalDayToSecondConstant() {
            this.fromInterval = Randomly.fromOptions(Interval.values());
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd HH:mm:ss");
            switch (fromInterval) {
            case DAY:
                dateFormat = new SimpleDateFormat("dd");
                break;
            case HOUR:
                dateFormat = new SimpleDateFormat("HH");
                break;
            case MINUTE:
                dateFormat = new SimpleDateFormat("mm");
                break;
            case SECOND:
                dateFormat = new SimpleDateFormat("ss");
                break;
            default:
                break;
            }

            Randomly rand = new Randomly();

            Timestamp timestamp = new Timestamp(rand.getLong(0, System.currentTimeMillis()));
            this.textRepresentation = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepresentation;
        }

        @Override
        public String toString() {
            // if (toInterval == null) {
            return String.format("INTERVAL '%s' %s", textRepresentation, fromInterval.name());
            // } else {
            // return String.format("INTERVAL '%s' %s TO %s", textRepresentation, fromInterval, toInterval);
            // }
        }

        private enum Interval {
            DAY, HOUR, MINUTE, SECOND
        }

    }

    public static class PrestoIntervalYearToMonthConstant extends PrestoConstant {

        public String textRepresentation;
        private final Interval fromInterval;

        public PrestoIntervalYearToMonthConstant() {
            fromInterval = Randomly.fromOptions(Interval.values());
            SimpleDateFormat dateFormat;
            switch (fromInterval) {
            case YEAR:
                dateFormat = new SimpleDateFormat("yyyy");
                break;
            case MONTH:
                dateFormat = new SimpleDateFormat("MM");
                break;
            default:
                dateFormat = new SimpleDateFormat("yyyy-MM");
            }

            Randomly rand = new Randomly();

            Timestamp timestamp = new Timestamp(rand.getLong(0, System.currentTimeMillis()));
            textRepresentation = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepresentation;
        }

        @Override
        public String toString() {
            return String.format("INTERVAL '%s' %s", textRepresentation, fromInterval.name());
        }

        private enum Interval {
            YEAR, MONTH
        }

    }

    public static class PrestoBooleanConstant extends PrestoConstant {

        private final boolean value;

        public PrestoBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean asBoolean() {
            return value;
        }

        @Override
        public boolean isBoolean() {
            return true;
        }

    }

    public static class PrestoArrayConstant extends PrestoConstant {

        private final List<Node<PrestoExpression>> elements;

        public PrestoArrayConstant(List<Node<PrestoExpression>> elements) {
            this.elements = new ArrayList<>(elements);
        }

        @Override
        public boolean isArray() {
            return true;
        }

        @Override
        public String toString() {
            return "ARRAY[" + elements.stream().map(Object::toString).collect(Collectors.joining(", ")) + "]";
        }

    }

}
