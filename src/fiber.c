
#define _GNU_SOURCE
#include <assert.h>
#include <unistd.h>
#include <signal.h>
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
#define NO_OF_HARVESTERS (4ULL)

/* NOTE : stack configuration can only get as big as 4GB*/
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

/* This saves calle saved non volatile registers for the x86 Architecture and the OS ABI function rdi needed for the first argumnent  */
struct  x86_registers {
	size_t rdi ;
	size_t rsp ; 
	size_t rbp ; 
	size_t rbx ; 
	size_t r12 ; 
	size_t r13 ; 
	size_t r14 ; 
	size_t r15 ; 
};

/* /#define DEFINE_RESULT(TypeName , DataType, Alignment ) \
	struct TypeName{\
		static_assert(sizeof(DataType) <= 64 , "The Data size is greatear than 64 byte alignement " );\
		_Atomic(uint8_t) m_mask ; \
		alignas(_Alignof(DataType)) DataType m_data ;\
	};
*/
struct ResultPayload{
		_Atomic(uint8_t) m_mask ; 
		alignas(_Alignof(size_t)) size_t m_data ;
};

struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ)))fiber_data{
		void*(*m_func_addr)(void*) ; /* Adrress of function */
		struct x86_registers m_registers ;
		uint32_t m_stack_position;
		uint8_t m_mask ; /* This contains the state of the function */
		void* m_result ;
	};
static_assert(sizeof(struct fiber_data) == 128ULL , "size of fiber data is not 128");






/* Harvester contains data of the cores that acts as consumers */
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ))) Harvester{
	// Local section of harvester only available to the Harvester 
	struct mpsc_queue* m_queue_info;
	/* m_local_buf uased when to free up spacer during colleasing */
	unsigned char* m_local_copy_buf ;
	unsigned char* m_stack ; /* local stack to the harvester */
	size_t m_stack_pos; 
	void* m_queue;
	uint32_t m_tail_idx ;
	uint32_t m_head_idx ; 
	uint32_t m_tail_rem_idx ;
	uint8_t mask ; /* Mask contain mask to free local buffer for coleasing  */
};

/*Fiber */
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ))) fiber {
	void* m_queue[NO_OF_HARVESTERS];/* Addrresses of all the queues*/
	pthread_t m_pids[NO_OF_HARVESTERS];
	struct mpsc_queue* m_queue_info[NO_OF_HARVESTERS];

};


struct __attribute__((aligned(CACHE_LINE))) fiber_client_local{
	void** m_addr; /* The address of the queue it is accesing */
	struct mpsc_queue** mqueue_info ;/* the global synchronization place */
	size_t  m_current_id ;
	uint32_t m_tail_idx ;
	uint32_t m_head_idx ; 
	uint32_t m_head_rem_idx ;

};

struct __attribute__((aligned(CACHE_LINE)))lh_t { /* Launch harvester type*/
	void* m_addr ;
	struct mpsc_queue* m_info ;
	uint16_t m_core ;
	pthread_t * m_pids;
};
/* Function Declerations*/

void  fiber_init(struct fiber* ); 
int fiber_deinit(struct fiber*) ;
int fiber_switch(struct fiber_data* , struct fiber_data*) ; 

/* FIber client*/
int fiber_client_init(struct fiber* , struct fiber_client_local*);

/* Function to set up a fiber data */
int fiber_client_trampoline( struct fiber_client_local* , void*(*func)(void*) ,void* arg ,  struct ResultPayload* result_ptr );

static void* launch_harvester(void* );
void  hrvst_deinit(int);
int hrvst_functions(struct Harvester* ); 
static int __hrvst_fragmentation_internal_check(struct Harvester*); 

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

	// setting up HArvesters/
	struct lh_t m_arg2 ; 
	for(size_t i = 0 ; i <= (size_t)NO_OF_HARVESTERS ; i++){
		if( pthread_attr_init(&attr2) < 0 ) {
			crash_fail();
		}
		m_arg2.m_addr = (fib->m_queue[i]) ;
		m_arg2.m_info = fib->m_queue_info[i];
		m_arg2.m_core = (uint16_t)i;
		m_arg2.m_pids = &(fib->m_pids[i]);
		ret =pthread_create(&pid , &attr2 , &launch_harvester , (void*)&m_arg2);
		if(ret <0 ) crash_fail() ; 
		pthread_detach(pid);
	}

}


int fiber_deinit(struct fiber* fib){
	if(!fib ) return -1;
	for(size_t i = 0 ; i < NO_OF_HARVESTERS ; i++){
		munlock(fib->m_queue[i], QUEUE_SIZE); 
		munmap(fib->m_queue , QUEUE_SIZE);
		free(fib->m_queue_info[i]);
		pthread_kill(fib->m_pids[i],SIGUSR1 );
	}
	return  0;
}


int fiber_client_init(struct fiber* fib, struct fiber_client_local* local  ){
	if(!fib  && !local)return -1;
	local->m_current_id = 0 ;
	local->m_addr = (fib->m_queue);
	local->mqueue_info = fib->m_queue_info;
	local->m_head_idx = 0 ; 
	local->m_tail_idx = 0 ;
	local->m_head_rem_idx = 0 ;
	return 0;
}
int __fiber__get_local_queue_synchronization_();


int fiber_client_add( struct fiber_client_local* local  , void*(*func)(void*) , void* arg , struct ResultPayload* result_ptr ){
	if(!local)return -1;
	if(local->m_head_rem_idx == 0 ){
		__fiber__get_local_queue_synchronization_();
	}
	auto id = local->m_current_id ;
	auto ptr = (struct fiber_data*)(local->m_addr[id]) ;
	auto head_idx = local->m_head_idx;
	ptr[head_idx].m_func_addr = func ;
	ptr[head_idx].m_registers.rdi = (size_t)arg ;
	ptr[head_idx].m_result = (void*) result_ptr;
	atomic_store_explicit(&(result_ptr->m_mask) , FIBST_INQUEUE,memory_order_release);
	local->m_head_idx++;

	local->m_head_rem_idx--;
	return 0;
}


static thread_local struct Harvester* ptr_local_harvester;
static void* launch_harvester(void* arg){
	if(!arg) return NULL ;
	struct Harvester local_harvester ;
	ptr_local_harvester = &local_harvester;
	struct sigaction sig  = {0};

	sig.sa_handler = &hrvst_deinit;
	if(sigaction(SIGUSR1,&sig, NULL ) == -1){
		crash_fail();
	}
	auto args = (struct lh_t*)arg;
	int ret ;
	cpu_set_t cpu_set ;
	CPU_ZERO(&cpu_set); 
	
	CPU_SET(args->m_core, &cpu_set);
	pthread_t pid = pthread_self();
	*args->m_pids = pid ;
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
	local_harvester.m_stack_pos = (uint64_t)STACK_SIZE;  
	local_harvester.m_tail_idx= 0 ; 
	local_harvester.m_head_idx = 0 ; 
	local_harvester.m_tail_rem_idx = 0 ;
	local_harvester.mask = 0x00 ;

	while(true){}	
	return NULL ;
}

// This si signal to kill the Harvester threads
void  hrvst_deinit(int){
	auto ptr_stack = ptr_local_harvester->m_stack;
	auto ptr_local =ptr_local_harvester->m_local_copy_buf ; 
	munlock(ptr_stack , STACK_SIZE);
	munlock(ptr_local , LOCAL_BUF_SZ);
	munmap(ptr_stack , STACK_SIZE) ;
	munmap(ptr_local,LOCAL_BUF_SZ );
	pthread_kill(pthread_self(), SIGKILL);
}
/* function fills the local consumer count from the global consumer count 
 * It will check if queue is full or empty and returns results based on the state of the queue 
 *  is returned on pass and -1 for error other functions for more serious cases 
 * */
static __attribute__((noinline)) int __hrvst_fill_local_harvester_queue();


// #define DEFINE_HRVST_FUNCTIONS(ResultType , ResDataType)
int hrvst_functions(struct Harvester* hrv){
	if(hrv->m_tail_rem_idx == 0 ){
		__hrvst_fill_local_harvester_queue();
		// possible return -1
	}
	auto tail = hrv->m_tail_idx;
	auto stack_ptr = hrv->m_stack;
	auto stk_pos = hrv->m_stack_pos ;
	struct fiber_data* ptr = (struct fiber_data*)(hrv->m_queue);
	void*  first_arg = (void*)(ptr[tail].m_registers.rdi) ;
	if(/*change*/ first_arg == NULL){/*check if mask is fresh runnign*/
		atomic_store_explicit(&( ((struct ResultPayload*)(ptr[tail].m_result))->m_mask) ,FIBST_RUNNING , memory_order_release );
		void* res = ptr[tail].m_func_addr(first_arg);
		((struct ResultPayload*)(ptr[tail].m_result))->m_data = (size_t)res;
		atomic_store_explicit(&( ((struct ResultPayload*)(ptr[tail].m_result))->m_mask) ,FIBST_FINSHED , memory_order_release );
		hrv->m_tail_rem_idx--;
		return 0;
	}else if ((size_t)first_arg == 90)/* this si for yielkded function resuming */{
	}
		return 0;
} 

int main(){

	return 0;
}
