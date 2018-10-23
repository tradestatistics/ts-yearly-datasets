--
-- Table structure for table attributes_country_names
--

DROP TABLE IF EXISTS public.attributes_country_names;
CREATE TABLE public.attributes_country_names (
  country_iso varchar(3) DEFAULT '' PRIMARY KEY NOT NULL,
  country_name_english varchar(255) DEFAULT NULL,
  country_fullname_english varchar(255) DEFAULT NULL,
  country_abbreviation varchar(255) DEFAULT NULL
);

--
-- Table structure for table public.attributes_product_names
--

DROP TABLE IF EXISTS attributes_product_names;
CREATE TABLE attributes_product_names (
  commodity_code varchar(4) DEFAULT '' PRIMARY KEY NOT NULL,
  product_fullname_english varchar(255) DEFAULT NULL,
  group_code varchar(2) DEFAULT NULL,
  group_name varchar(255) DEFAULT NULL,
  color varchar(7) DEFAULT NULL
);

--
-- Table structure for table public.hs07_yp
--

DROP TABLE IF EXISTS hs07_yp;
CREATE TABLE hs07_yp (
  year integer NOT NULL,
  partner_iso varchar(3) NOT NULL,
  export_value_usd decimal DEFAULT NULL,
  import_value_usd decimal DEFAULT NULL,
  export_value_usd_change_1year decimal DEFAULT NULL,
  export_value_usd_change_5years decimal DEFAULT NULL,
  export_value_usd_percentage_change_1year float DEFAULT NULL,
  export_value_usd_percentage_change_5years float DEFAULT NULL,
  import_value_usd_change_1year decimal DEFAULT NULL,
  import_value_usd_change_5years decimal DEFAULT NULL,
  import_value_usd_percentage_change_1year float DEFAULT NULL,
  import_value_usd_percentage_change_5years float DEFAULT NULL,
  CONSTRAINT hs07_yp_pk PRIMARY KEY (year, partner_iso),
  CONSTRAINT hs07_yp_attributes_country_names_fk FOREIGN KEY (partner_iso) REFERENCES public.attributes_country_names (country_iso)
);

--
-- Table structure for table public.hs07_ypc
--

DROP TABLE IF EXISTS hs07_ypc;
CREATE TABLE hs07_ypc (
  year integer NOT NULL,
  partner_iso varchar(3) NOT NULL,
  commodity_code varchar(4) NOT NULL,
  export_value_usd decimal DEFAULT NULL,
  import_value_usd decimal DEFAULT NULL,
  export_value_usd_change_1year decimal DEFAULT NULL,
  export_value_usd_change_5years decimal DEFAULT NULL,
  export_value_usd_percentage_change_1year float DEFAULT NULL,
  export_value_usd_percentage_change_5years float DEFAULT NULL,
  import_value_usd_change_1year decimal DEFAULT NULL,
  import_value_usd_change_5years decimal DEFAULT NULL,
  import_value_usd_percentage_change_1year float DEFAULT NULL,
  import_value_usd_percentage_change_5years float DEFAULT NULL,
  CONSTRAINT hs07_ypc_pk PRIMARY KEY (year, partner_iso, commodity_code),
  CONSTRAINT hs07_ypc_attributes_country_names_fk FOREIGN KEY (partner_iso) REFERENCES public.attributes_country_names (country_iso),
  CONSTRAINT hs07_ypc_attributes_product_names_fk_2 FOREIGN KEY (commodity_code) REFERENCES public.attributes_product_names (commodity_code)
);

--
-- Table structure for table public.hs07_yr
--

DROP TABLE IF EXISTS hs07_yr;
CREATE TABLE hs07_yr (
  year integer NOT NULL,
  reporter_iso varchar(3) NOT NULL,
  export_value_usd decimal DEFAULT NULL,
  import_value_usd decimal DEFAULT NULL,
  top_export_commodity_code varchar(4) DEFAULT NULL,
  top_export_value_usd decimal DEFAULT NULL,
  top_import_commodity_code varchar(4) DEFAULT NULL,
  top_import_value_usd decimal DEFAULT NULL,
  export_value_usd_change_1year decimal DEFAULT NULL,
  export_value_usd_change_5years decimal DEFAULT NULL,
  export_value_usd_percentage_change_1year float DEFAULT NULL,
  export_value_usd_percentage_change_5years float DEFAULT NULL,
  import_value_usd_change_1year decimal DEFAULT NULL,
  import_value_usd_change_5years decimal DEFAULT NULL,
  import_value_usd_percentage_change_1year float DEFAULT NULL,
  import_value_usd_percentage_change_5years float DEFAULT NULL,
  CONSTRAINT hs07_yr_pk PRIMARY KEY (year, reporter_iso),
  CONSTRAINT hs07_yr_attributes_country_names_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_country_names (country_iso)
);

--
-- Table structure for table public.hs07_yrp
--

DROP TABLE IF EXISTS hs07_yrp;
CREATE TABLE hs07_yrp (
  year integer NOT NULL,
  reporter_iso varchar(3) NOT NULL,
  partner_iso varchar(3) NOT NULL,
  export_value_usd decimal DEFAULT NULL,
  import_value_usd decimal DEFAULT NULL,
  export_value_usd_change_1year decimal DEFAULT NULL,
  export_value_usd_change_5years decimal DEFAULT NULL,
  export_value_usd_percentage_change_1year float DEFAULT NULL,
  export_value_usd_percentage_change_5years float DEFAULT NULL,
  import_value_usd_change_1year decimal DEFAULT NULL,
  import_value_usd_change_5years decimal DEFAULT NULL,
  import_value_usd_percentage_change_1year float DEFAULT NULL,
  import_value_usd_percentage_change_5years float DEFAULT NULL,
  CONSTRAINT hs07_yrp_pk PRIMARY KEY (year, reporter_iso, partner_iso),
  CONSTRAINT hs07_yrp_attributes_country_names_id_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_country_names (country_iso),
  CONSTRAINT hs07_yrp_attributes_country_names_id_fk_2 FOREIGN KEY (partner_iso) REFERENCES public.attributes_country_names (country_iso)
);

--
-- Table structure for table public.hs07_yrpc
--

DROP TABLE IF EXISTS hs07_yrpc;
CREATE TABLE hs07_yrpc (
  year integer NOT NULL,
  reporter_iso varchar(3) NOT NULL,
  partner_iso varchar(3) NOT NULL,
  commodity_code varchar(4) NOT NULL,
  commodity_code_length integer DEFAULT NULL,
  export_value_usd decimal DEFAULT NULL,
  import_value_usd decimal DEFAULT NULL,
  export_value_usd_change_1year decimal DEFAULT NULL,
  export_value_usd_change_5years decimal DEFAULT NULL,
  export_value_usd_percentage_change_1year float DEFAULT NULL,
  export_value_usd_percentage_change_5years float DEFAULT NULL,
  import_value_usd_change_1year decimal DEFAULT NULL,
  import_value_usd_change_5years decimal DEFAULT NULL,
  import_value_usd_percentage_change_1year float DEFAULT NULL,
  import_value_usd_percentage_change_5years float DEFAULT NULL,
  CONSTRAINT hs07_yrpc_pk PRIMARY KEY (year, reporter_iso, partner_iso, commodity_code),
  CONSTRAINT hs07_yrpc_attributes_country_names_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_country_names (country_iso),
  CONSTRAINT hs07_yrpc_attributes_country_names_fk_2 FOREIGN KEY (partner_iso) REFERENCES public.attributes_country_names (country_iso),
  CONSTRAINT hs07_yrpc_attributes_product_names_fk_3 FOREIGN KEY (commodity_code) REFERENCES public.attributes_product_names (commodity_code)
);

--
-- Table structure for table hs07_yrc
--

DROP TABLE IF EXISTS hs07_yrc;
CREATE TABLE hs07_yrc (
  year integer NOT NULL,
  reporter_iso varchar(3) NOT NULL,
  commodity_code varchar(4) NOT NULL,
  export_value_usd decimal DEFAULT NULL,
  import_value_usd decimal DEFAULT NULL,
  export_rca float DEFAULT NULL,
  import_rca float DEFAULT NULL,
  export_value_usd_change_1year decimal DEFAULT NULL,
  export_value_usd_change_5years decimal DEFAULT NULL,
  export_value_usd_percentage_change_1year float DEFAULT NULL,
  export_value_usd_percentage_change_5years float DEFAULT NULL,
  import_value_usd_change_1year decimal DEFAULT NULL,
  import_value_usd_change_5years decimal DEFAULT NULL,
  import_value_usd_percentage_change_1year float DEFAULT NULL,
  import_value_usd_percentage_change_5years float DEFAULT NULL,
  CONSTRAINT hs07_yrc_pk PRIMARY KEY (year, reporter_iso, commodity_code),
  CONSTRAINT hs07_yrc_attributes_country_names_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_country_names (country_iso),
  CONSTRAINT hs07_yrc_attributes_product_names_fk_2 FOREIGN KEY (commodity_code) REFERENCES public.attributes_product_names (commodity_code)
);

--
-- Table structure for table hs07_yp
--

DROP TABLE IF EXISTS hs07_yc;
CREATE TABLE hs07_yc (
  year integer NOT NULL,
  commodity_code varchar(4) NOT NULL,
  export_value_usd decimal DEFAULT NULL,
  import_value_usd decimal DEFAULT NULL,
  pci float DEFAULT NULL,
  pci_rank integer DEFAULT NULL,
  pci_rank_delta integer DEFAULT NULL,
  top_exporter_iso varchar(3) DEFAULT NULL,
  top_importer_iso varchar(3) DEFAULT NULL,
  export_value_usd_change_1year decimal DEFAULT NULL,
  export_value_usd_change_5years decimal DEFAULT NULL,
  export_value_usd_percentage_change_1year float DEFAULT NULL,
  export_value_usd_percentage_change_5years float DEFAULT NULL,
  import_value_usd_change_1year decimal DEFAULT NULL,
  import_value_usd_change_5years decimal DEFAULT NULL,
  import_value_usd_percentage_change_1year float DEFAULT NULL,
  import_value_usd_percentage_change_5years float DEFAULT NULL,
  CONSTRAINT hs07_yc_pk PRIMARY KEY (year, commodity_code),
  CONSTRAINT hs07_yc_attributes_product_names_fk FOREIGN KEY (commodity_code) REFERENCES public.attributes_product_names (commodity_code)
);
