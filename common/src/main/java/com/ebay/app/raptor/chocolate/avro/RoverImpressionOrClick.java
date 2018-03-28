/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.ebay.app.raptor.chocolate.avro;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class RoverImpressionOrClick extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -1430895030130443827L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"RoverImpressionOrClick\",\"namespace\":\"com.ebay.app.raptor.chocolate.avro\",\"fields\":[{\"name\":\"tag\",\"type\":{\"type\":\"enum\",\"name\":\"Tag\",\"symbols\":[\"IMPRESSIONS\",\"CLICKS\"]}},{\"name\":\"snapshot_id\",\"type\":\"long\"},{\"name\":\"publisher_id\",\"type\":\"long\"},{\"name\":\"campaign_id\",\"type\":\"long\"}],\"pk\":[\"snapshot_id\"]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<RoverImpressionOrClick> ENCODER =
      new BinaryMessageEncoder<RoverImpressionOrClick>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<RoverImpressionOrClick> DECODER =
      new BinaryMessageDecoder<RoverImpressionOrClick>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<RoverImpressionOrClick> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<RoverImpressionOrClick> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<RoverImpressionOrClick>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this RoverImpressionOrClick to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a RoverImpressionOrClick from a ByteBuffer. */
  public static RoverImpressionOrClick fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private com.ebay.app.raptor.chocolate.avro.Tag tag;
   private long snapshot_id;
   private long publisher_id;
   private long campaign_id;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public RoverImpressionOrClick() {}

  /**
   * All-args constructor.
   * @param tag The new value for tag
   * @param snapshot_id The new value for snapshot_id
   * @param publisher_id The new value for publisher_id
   * @param campaign_id The new value for campaign_id
   */
  public RoverImpressionOrClick(com.ebay.app.raptor.chocolate.avro.Tag tag, java.lang.Long snapshot_id, java.lang.Long publisher_id, java.lang.Long campaign_id) {
    this.tag = tag;
    this.snapshot_id = snapshot_id;
    this.publisher_id = publisher_id;
    this.campaign_id = campaign_id;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return tag;
    case 1: return snapshot_id;
    case 2: return publisher_id;
    case 3: return campaign_id;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: tag = (com.ebay.app.raptor.chocolate.avro.Tag)value$; break;
    case 1: snapshot_id = (java.lang.Long)value$; break;
    case 2: publisher_id = (java.lang.Long)value$; break;
    case 3: campaign_id = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'tag' field.
   * @return The value of the 'tag' field.
   */
  public com.ebay.app.raptor.chocolate.avro.Tag getTag() {
    return tag;
  }

  /**
   * Sets the value of the 'tag' field.
   * @param value the value to set.
   */
  public void setTag(com.ebay.app.raptor.chocolate.avro.Tag value) {
    this.tag = value;
  }

  /**
   * Gets the value of the 'snapshot_id' field.
   * @return The value of the 'snapshot_id' field.
   */
  public java.lang.Long getSnapshotId() {
    return snapshot_id;
  }

  /**
   * Sets the value of the 'snapshot_id' field.
   * @param value the value to set.
   */
  public void setSnapshotId(java.lang.Long value) {
    this.snapshot_id = value;
  }

  /**
   * Gets the value of the 'publisher_id' field.
   * @return The value of the 'publisher_id' field.
   */
  public java.lang.Long getPublisherId() {
    return publisher_id;
  }

  /**
   * Sets the value of the 'publisher_id' field.
   * @param value the value to set.
   */
  public void setPublisherId(java.lang.Long value) {
    this.publisher_id = value;
  }

  /**
   * Gets the value of the 'campaign_id' field.
   * @return The value of the 'campaign_id' field.
   */
  public java.lang.Long getCampaignId() {
    return campaign_id;
  }

  /**
   * Sets the value of the 'campaign_id' field.
   * @param value the value to set.
   */
  public void setCampaignId(java.lang.Long value) {
    this.campaign_id = value;
  }

  /**
   * Creates a new RoverImpressionOrClick RecordBuilder.
   * @return A new RoverImpressionOrClick RecordBuilder
   */
  public static com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder newBuilder() {
    return new com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder();
  }

  /**
   * Creates a new RoverImpressionOrClick RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new RoverImpressionOrClick RecordBuilder
   */
  public static com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder newBuilder(com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder other) {
    return new com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder(other);
  }

  /**
   * Creates a new RoverImpressionOrClick RecordBuilder by copying an existing RoverImpressionOrClick instance.
   * @param other The existing instance to copy.
   * @return A new RoverImpressionOrClick RecordBuilder
   */
  public static com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder newBuilder(com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick other) {
    return new com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder(other);
  }

  /**
   * RecordBuilder for RoverImpressionOrClick instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<RoverImpressionOrClick>
    implements org.apache.avro.data.RecordBuilder<RoverImpressionOrClick> {

    private com.ebay.app.raptor.chocolate.avro.Tag tag;
    private long snapshot_id;
    private long publisher_id;
    private long campaign_id;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.tag)) {
        this.tag = data().deepCopy(fields()[0].schema(), other.tag);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.snapshot_id)) {
        this.snapshot_id = data().deepCopy(fields()[1].schema(), other.snapshot_id);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.publisher_id)) {
        this.publisher_id = data().deepCopy(fields()[2].schema(), other.publisher_id);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.campaign_id)) {
        this.campaign_id = data().deepCopy(fields()[3].schema(), other.campaign_id);
        fieldSetFlags()[3] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing RoverImpressionOrClick instance
     * @param other The existing instance to copy.
     */
    private Builder(com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.tag)) {
        this.tag = data().deepCopy(fields()[0].schema(), other.tag);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.snapshot_id)) {
        this.snapshot_id = data().deepCopy(fields()[1].schema(), other.snapshot_id);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.publisher_id)) {
        this.publisher_id = data().deepCopy(fields()[2].schema(), other.publisher_id);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.campaign_id)) {
        this.campaign_id = data().deepCopy(fields()[3].schema(), other.campaign_id);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'tag' field.
      * @return The value.
      */
    public com.ebay.app.raptor.chocolate.avro.Tag getTag() {
      return tag;
    }

    /**
      * Sets the value of the 'tag' field.
      * @param value The value of 'tag'.
      * @return This builder.
      */
    public com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder setTag(com.ebay.app.raptor.chocolate.avro.Tag value) {
      validate(fields()[0], value);
      this.tag = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'tag' field has been set.
      * @return True if the 'tag' field has been set, false otherwise.
      */
    public boolean hasTag() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'tag' field.
      * @return This builder.
      */
    public com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder clearTag() {
      tag = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'snapshot_id' field.
      * @return The value.
      */
    public java.lang.Long getSnapshotId() {
      return snapshot_id;
    }

    /**
      * Sets the value of the 'snapshot_id' field.
      * @param value The value of 'snapshot_id'.
      * @return This builder.
      */
    public com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder setSnapshotId(long value) {
      validate(fields()[1], value);
      this.snapshot_id = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'snapshot_id' field has been set.
      * @return True if the 'snapshot_id' field has been set, false otherwise.
      */
    public boolean hasSnapshotId() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'snapshot_id' field.
      * @return This builder.
      */
    public com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder clearSnapshotId() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'publisher_id' field.
      * @return The value.
      */
    public java.lang.Long getPublisherId() {
      return publisher_id;
    }

    /**
      * Sets the value of the 'publisher_id' field.
      * @param value The value of 'publisher_id'.
      * @return This builder.
      */
    public com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder setPublisherId(long value) {
      validate(fields()[2], value);
      this.publisher_id = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'publisher_id' field has been set.
      * @return True if the 'publisher_id' field has been set, false otherwise.
      */
    public boolean hasPublisherId() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'publisher_id' field.
      * @return This builder.
      */
    public com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder clearPublisherId() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'campaign_id' field.
      * @return The value.
      */
    public java.lang.Long getCampaignId() {
      return campaign_id;
    }

    /**
      * Sets the value of the 'campaign_id' field.
      * @param value The value of 'campaign_id'.
      * @return This builder.
      */
    public com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder setCampaignId(long value) {
      validate(fields()[3], value);
      this.campaign_id = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'campaign_id' field has been set.
      * @return True if the 'campaign_id' field has been set, false otherwise.
      */
    public boolean hasCampaignId() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'campaign_id' field.
      * @return This builder.
      */
    public com.ebay.app.raptor.chocolate.avro.RoverImpressionOrClick.Builder clearCampaignId() {
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RoverImpressionOrClick build() {
      try {
        RoverImpressionOrClick record = new RoverImpressionOrClick();
        record.tag = fieldSetFlags()[0] ? this.tag : (com.ebay.app.raptor.chocolate.avro.Tag) defaultValue(fields()[0]);
        record.snapshot_id = fieldSetFlags()[1] ? this.snapshot_id : (java.lang.Long) defaultValue(fields()[1]);
        record.publisher_id = fieldSetFlags()[2] ? this.publisher_id : (java.lang.Long) defaultValue(fields()[2]);
        record.campaign_id = fieldSetFlags()[3] ? this.campaign_id : (java.lang.Long) defaultValue(fields()[3]);
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<RoverImpressionOrClick>
    WRITER$ = (org.apache.avro.io.DatumWriter<RoverImpressionOrClick>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<RoverImpressionOrClick>
    READER$ = (org.apache.avro.io.DatumReader<RoverImpressionOrClick>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
