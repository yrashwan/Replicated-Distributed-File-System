package utilities;

public final class Pair <E1, E2>{
	
	private final E1 first;
	private final E2 second;

	public Pair(E1 first, E2 second){
		this.first = first;
		this.second = second;
	}
		
	public E1 first(){
		return first;
	}
	
	public E2 second(){
		return second;
	}
}
