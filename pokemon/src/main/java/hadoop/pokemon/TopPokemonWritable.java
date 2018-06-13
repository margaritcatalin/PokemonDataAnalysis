package hadoop.pokemon;

	import java.io.DataInput;
	import java.io.DataOutput;
	import java.io.IOException;

	import org.apache.hadoop.io.WritableComparable;

	public class TopPokemonWritable implements WritableComparable<TopPokemonWritable>{
		private String pokemonName;
		private Long powerValue;
		public TopPokemonWritable() {}
		public TopPokemonWritable(String pokemonName, Long powerValue) {
			this.set(pokemonName, powerValue);
		}
		public void set(String pokemonName, Long powerValue) {
			this.pokemonName = pokemonName;
			this.powerValue = powerValue;
		}
		public String getPokemonName() {
			return pokemonName;
		}
		public Long getpowerValue() {
			return powerValue;
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(pokemonName);
			out.writeLong(powerValue);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			this.pokemonName = in.readUTF();
			this.powerValue = in.readLong();
		}
		@Override
		public int compareTo(TopPokemonWritable o) {
			int minus = this.getpowerValue().compareTo(o.getpowerValue());
			if(minus != 0) {
				return -minus;
			}
			return -(this.getPokemonName().compareTo(o.getPokemonName()));
		}
		@Override
		public String toString() {
			return pokemonName+"\t"+powerValue;
		}
		@Override
		public int hashCode() {
			return super.hashCode();
		}
		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}	
	}
