
#define _GNU_SOURCE
#include <assert.h>
#include <unistd.h>
#include <sched.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/mman.h>
#include <stdlib.h> 
#include <string.h> 
#include <pthread.h> 
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
/* Error List*/ 
[[maybe_unused]] constexpr int FIB_ERRFAIL = -1;
/* Enum Definitions */


/* Enum state of FIber state */ 
enum Fib_state_ID : uint8_t {
	FIBST_RUNNING ,
	FIBST_INQUEUE, 
	FIBST_YIELDING,
	FIBST_FINSHED 
};


enum lh_mask : uint8_t{
	LH_WAITING  = 0x00, 
	LH_READY = 0x01
};
/* NEEDED TEMPLATE MACROS*/

#define CACHE_LINE 64ULL
#define HD_DESTRUCTIVE_INF_SZ (CACHE_LINE* 2ULL ) /* Due to agressive prefetchers on modern processor the Alignment for destructive infrence size will be set to the cache line * 2 */

/* Value of number of cores are set at compiile time to chnage this define your own */
#define NO_OF_THREADS 4ULL
#define NO_OF_HARVESTERS (NO_OF_THREADS -1ULL)
#define STACK_SIZE_DEFAULT 64ULL * 1024ULL /* 64KB Default configuration */
#define STACK_SIZE (STACK_SIZE_DEFAULT)
#define QUEUE_NO 1024ULL
#define QUEUE_SIZE (QUEUE_NO * 128ULL)
#define RESULT_QUEUE_SZ  (64ULL * QUEUE_SIZE )
#define LOCAL_BUF_MAX_CNT 100ULL 
#define LOCAL_BUF_SZ (LOCAL_BUF_MAX_CNT * 128ULL) 
#define MASK_4KB (0xFFFULL)
#define MASK_63BIT_L (0x8000000000000000ULL)
#define MASK_63BIT (~MASK_63BIT_L) 
/* Struct Defintions */ 

struct mpsc_queue{

	/* Head written to by the scheduler 
	 * NOTE : DUE to the large amoutn of producer this will cause great contention on the head_ atomic value and will sat		urate the buss local copy will be kept but also only the scheduler after collation of result from the producer cores w		ill write to this varaible 
	 */ 
	alignas(HD_DESTRUCTIVE_INF_SZ) _Atomic(uint64_t) m_head_  ;

	/* tail is consume and gooubled up by the consumer*/
	alignas(HD_DESTRUCTIVE_INF_SZ) _Atomic(uint64_t) m_tail_ ;
};

/* This saves calle saved non volatile registers for the x86 Architecture */
struct x86_nonV_registers {
	size_t rsp ; 
	size_t rbp ; 
	size_t rbx ; 
	size_t r12 ; 
	size_t r13 ; 
	size_t r14 ; 
	size_t r15 ; 
};

/* This contains the values for being entered into the function 
 * NOTE FOR SPACE CONSTRAINS FUNCTIONS WIL ONLY SUPPORT */ 
struct x86_abi_func_registers{
 	size_t rdi , rsi , rdx , rcx , r8;	
};


struct __attribute__((aligned(CACHE_LINE)))fiber_data{
	void*(*m_func_addr)(void*) ; /* Adrress of function */
	struct x86_nonV_registers m_registers ;
	void* m_result ;
	uint8_t m_mask ; /* This contains the state of the function */
};

#define DEFINE_RESULT(TypeName , DataType, Alignment ) \
	struct TypeName{\
		static_assert(sizeof(DataType) <= 64 , "The Data size is greatear than 64 byte alignement " );\
		_Atomic(uint8_t) m_mask ; \
		alignas(_Alignof(DataType)) DataType m_data ;\
	}




/* Harvester contains data of the cores that acts as consumers */
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ))) Harvester{
	// Local section of harvester only available to the Harvester 
	struct mpsc_queue* m_queue_info;
	/* m_local_buf uased when to free up spacer during colleasing */
	unsigned char* m_local_copy_buf ;
	unsigned char* m_stack ; /* local stack to the harvester */
	void* m_queue;
	uint32_t m_tail_idx ;
	uint32_t m_head_idx ; 
	uint32_t m_tail_rem_idx ;
	uint8_t mask ; /* Mask contain mask to free local buffer for coleasing  */
};

/*Fiber */
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ))) fiber {
	void* m_queue[NO_OF_HARVESTERS];/* Addrresses of all the queues*/
	struct mpsc_queue* m_queue_info[NO_OF_HARVESTERS]; 
};


struct __attribute__((aligned(CACHE_LINE))) fiber_client_local{
	void* m_addr; /* The address of the queue it is accesing */
	struct mpsc_queue* mqueue_info ;/* the global synchronization place */
	size_t  m_current_id ;
	uint32_t m_tail_idx ;
	uint32_t m_head_idx ; 
	uint32_t m_head_rem_idx ;

};

struct __attribute__((aligned(CACHE_LINE)))lh_t { /* Launch harvester type*/
	void* m_addr ;
	struct mpsc_queue* m_info ;
	uint16_t m_core ;
};
/* Function Declerations*/

void  fiber_init(struct fiber* ); 
int fiber_deinit(struct fiber*) ;
int fiber_switch(struct fiber_data* , struct fiber_data*) ; 
int fiber_trampoline(struct fiber_data*);/* set up a fiber_data*/

/* FIber client*/
int fiber_client_init(struct fiber* , struct fiber_client_local*);
int fiber_client_add( struct fiber_client_local* , void*(*func)(void*) /*result fshould be here a pointer*/);

static void* launch_harvester(void* );
int harvester_deinit(struct Harvester*);
int harvest_functions(struct Harvester* ); 
static int __hv_fragmentation_internal_check(struct Harvester*); 

//------------------------------------------------
// HELPER FUNCTIONS 

__attribute__((always_inline)) static inline  bool extract_bit_inaddr(uint64_t addr){
	return (uint8_t)(((addr) & MASK_63BIT_L) >> 63ULL);
}	
__attribute__((always_inline)) static inline void*  swap_bit_inaddr(uint64_t addr){
	return  (void*)(((addr) ^ MASK_63BIT_L )  | ((addr) & MASK_63BIT)) ;
}

__attribute__((noinline , cold)) void crash_fail(){
	fprintf(stderr , "Error intilization ") ; 
	_exit(1) ;
}


__attribute__((noinline , cold)) void crash_fail_cpu(){
	fprintf(stderr , "Failed to lock cpu : sorry  ") ; 
	_exit(1) ;
}


/*Function defintions */


void  fiber_init(struct fiber* fib  ){
	if(!fib){crash_fail();}
	
	// Set up queues and result queuue memory space 
	for(size_t i = 0 ; i < NO_OF_HARVESTERS ; i++){
		void* ptr ;
		ptr  = mmap(NULL, QUEUE_SIZE, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
		if(ptr == MAP_FAILED){
			crash_fail();
		}
		fib->m_queue[i] = ptr ;
		ptr = aligned_alloc(_Alignof(struct mpsc_queue), sizeof(struct mpsc_queue));
		if(ptr == nullptr) {
			crash_fail();
		}
		fib->m_queue_info[i] =	ptr ; 
	}

	// Set up the scheduler 
	pthread_t pid ;
	int ret ;
	pthread_attr_t attr2 ;

	// setting up HArvesters
	struct lh_t m_arg2 ; 
	for(size_t i = 0 ; i <= (size_t)NO_OF_HARVESTERS ; i++){
		if( pthread_attr_init(&attr2) < 0 ) {
			crash_fail();
		}
		m_arg2.m_addr = (fib->m_queue[i]) ;
		m_arg2.m_info = fib->m_queue_info[i];
		m_arg2.m_core = (uint16_t)i;
		ret =pthread_create(&pid , &attr2 , &launch_harvester , (void*)&m_arg2);
		if(ret <0 ) crash_fail() ; 
		pthread_detach(pid);
	}

}


int fiber_client_init(struct fiber* fib, struct fiber_client_local* local  ){
	if(!fib  && !local)return -1;
	local->m_current_id = 0 ;
	local->m_addr = (fib->m_queue);
	local->mqueue_info = (struct mpsc_queue*)fib->m_queue_info;
	local->m_head_idx = 0 ; 
	local->m_tail_idx = 0 ;
	local->m_head_rem_idx = 0 ;
	return 0;
}
static void* launch_harvester(void* arg){
	if(!arg) return NULL ;
	struct Harvester local_harvester ;
	auto args = (struct lh_t*)arg;
	int ret ;
	cpu_set_t cpu_set ;
	CPU_ZERO(&cpu_set); 
	
	CPU_SET(args->m_core, &cpu_set);
	pthread_t pid = pthread_self();
	// lock core
	ret =  pthread_setaffinity_np(pid , sizeof(cpu_set_t) , &cpu_set);

	if(ret != 0 ){
		crash_fail_cpu();	
	}
	local_harvester.m_queue_info = args->m_info;
	/*set up stack */ 
	/* stack default to 64KB*/
	auto ptr = mmap(NULL , STACK_SIZE , PROT_READ| PROT_WRITE , MAP_ANONYMOUS | MAP_PRIVATE , -1 , 0 );
	if(ptr == MAP_FAILED) {
		crash_fail();
	}
	local_harvester.m_stack = ptr ;
	ret = mlock(ptr , STACK_SIZE);
	if(ret == -1){
		crash_fail();
	}
	// Perfrom hot looping here 
	// lock memery
	ret = mlock(args->m_addr , QUEUE_SIZE);
	if(ret == -1){
		crash_fail();
	}
	
	// Perfrom hot looping here 
	local_harvester.m_queue = args->m_addr ; 
	/*Set up local copy buffer */
	ptr = mmap(NULL ,LOCAL_BUF_SZ , PROT_READ| PROT_WRITE , MAP_ANONYMOUS | MAP_PRIVATE , -1 , 0 );
	if(ptr == MAP_FAILED) {
		crash_fail();
	}
	if(ptr == MAP_FAILED){crash_fail();}
	local_harvester.m_local_copy_buf = ptr; 
	local_harvester.m_tail_idx= 0 ; 
	local_harvester.m_head_idx = 0 ; 
	local_harvester.m_tail_rem_idx = 0 ;
	local_harvester.mask = 0x00 ;

	while(true){}	
	return NULL ;
}



int main(){

	return 0;
}
